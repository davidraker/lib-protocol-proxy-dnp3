"""One DNP3 outstation served by the proxy (the ``outstation`` role).

The served values live in a dnp3py ``Database``: a point table with values, quality flags and event buffers that the
``Outstation`` answers the master's reads from. Platform writes (``WRITE_POINTS``) update the database, which also
raises events for event-class points; the master's controls reach :class:`ServedCommandHandler`, which stores the new
output value and pushes it to the manager so the platform sees the master's write at once. ``READ_POINTS`` reports the
current served values. Everything is keyed by the full point topics the caller registered, as in the master role.

What dnp3py's ``OutstationTcpRunner`` does not do is transmit unsolicited responses on its own (``generate_unsolicited``
is never called by the runner), so a served outstation answers class polls, which collect the buffered events, but does
not initiate them.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, fields
from types import SimpleNamespace
from typing import Any, Iterable

from dnp3.core.enums import ControlCode
from dnp3.core.flags import AnalogQuality, BinaryQuality, CounterQuality
from dnp3.database import (AnalogInputConfig, AnalogOutputConfig, BinaryInputConfig, BinaryOutputConfig, CounterConfig,
                           Database, DatabaseConfig, EventClass)
from dnp3.objects.layout import PointKind
from dnp3.outstation import (CommandResult, DefaultCommandHandler, Outstation, OutstationConfig, OutstationTcpRunner,
                             UnsolicitedConfig)

from .client import OUTPUT_KINDS, PointSpec, PushCallback
from .multiplex import SELECTOR, Address, Multiplexer, address_of

_log = logging.getLogger(__name__)

MAX_POINTS_PER_KIND = 65536
BIND_TIMEOUT = 5.0
ON_CODES = (ControlCode.LATCH_ON, ControlCode.PULSE_ON)
OFF_CODES = (ControlCode.LATCH_OFF, ControlCode.PULSE_OFF)


@dataclass(frozen=True)
class _Kind:
    """How one point kind is held in the database."""
    store: str
    adder: str
    updater: str | None
    config: type
    quality: type
    python_type: type


KINDS: dict[PointKind, _Kind] = {
    PointKind.BINARY_INPUT: _Kind('binary_inputs', 'add_binary_input', 'update_binary_input', BinaryInputConfig, BinaryQuality, bool),
    PointKind.BINARY_OUTPUT: _Kind('binary_outputs', 'add_binary_output', 'update_binary_output', BinaryOutputConfig, BinaryQuality, bool),
    PointKind.ANALOG_INPUT: _Kind('analog_inputs', 'add_analog_input', 'update_analog_input', AnalogInputConfig, AnalogQuality, float),
    PointKind.ANALOG_OUTPUT: _Kind('analog_outputs', 'add_analog_output', 'update_analog_output', AnalogOutputConfig, AnalogQuality, float),
    PointKind.COUNTER: _Kind('counters', 'add_counter', 'update_counter', CounterConfig, CounterQuality, int),
    PointKind.FROZEN_COUNTER: _Kind('frozen_counters', 'add_frozen_counter', None, CounterConfig, CounterQuality, int),
}


@dataclass(frozen=True)
class ServedPoint(PointSpec):
    """A registered served point: the PointSpec plus what serving it needs."""
    event_class: int = 1
    remote_writable: bool = False
    initial_value: Any = None
    readback: Address | None = None      # the input that shows this output's value back; copied on every write

    @classmethod
    def from_dict(cls, spec: dict) -> 'ServedPoint':
        base = PointSpec.from_dict(spec)
        if base.kind not in KINDS:
            raise ValueError(f'{base.topic}: group {base.group} cannot be served by an outstation')
        event_class = spec.get('event_class')
        event_class = 1 if event_class in (None, '') else int(event_class)
        if event_class not in (0, 1, 2, 3):
            raise ValueError(f'{base.topic}: event class {event_class} is not 0 to 3')
        remote_writable = spec.get('remote_writable')
        remote_writable = base.kind in OUTPUT_KINDS if remote_writable in (None, '') else bool(remote_writable)
        readback = address_of(spec.get('readback'))
        if readback is not None and base.kind not in OUTPUT_KINDS:
            raise ValueError(f'{base.topic}: only an output names a read-back input')
        return cls(**{f.name: getattr(base, f.name) for f in fields(base)}, event_class=event_class,
                   remote_writable=remote_writable, initial_value=spec.get('initial_value'), readback=readback)

    @property
    def held(self) -> _Kind:
        return KINDS[self.kind]

    def raw_typed(self, value: Any):
        """The database-side value for an engineering value: scaled for analogs and counters, typed for the kind."""
        python_type = self.held.python_type
        if python_type is bool:
            return _as_bool(value)
        if python_type is int:
            return int(round(self.raw(float(value))))
        if isinstance(value, bool):
            raise TypeError('a boolean is not a number')
        return self.raw(float(value))


class ServedCommandHandler(DefaultCommandHandler):
    """Accepts the master's controls on served, remote-writable outputs; stores and pushes the new value."""

    def __init__(self, served: 'ServedOutstation'):
        self.served = served

    def _check(self, kind: PointKind, index: int) -> tuple[ServedPoint | None, CommandResult | None]:
        spec = self.served.spec_at(kind, index)
        if spec is None:
            return None, CommandResult.not_supported('not a served point')
        if not spec.remote_writable:
            return spec, CommandResult.blocked('not remotely writable')
        return spec, None

    def _binary(self, index: int, code: ControlCode, operate: bool) -> CommandResult:
        if code not in ON_CODES and code not in OFF_CODES:
            return CommandResult.not_supported(f'control code {code!r} not supported')
        address = (PointKind.BINARY_OUTPUT, index)
        if self.served.mux.handles(address):
            return self.served.mux.write_from_master(address, code in ON_CODES) if operate else CommandResult.success()
        spec, refusal = self._check(PointKind.BINARY_OUTPUT, index)
        if refusal is not None:
            return refusal
        if operate:
            self.served.store(spec, code in ON_CODES, from_master=True)
        return CommandResult.success()

    def select_binary_output(self, index, code, count, on_time, off_time):
        return self._binary(index, code, operate=False)

    def operate_binary_output(self, index, code, count, on_time, off_time, select_sequence):
        return self._binary(index, code, operate=True)

    def direct_operate_binary_output(self, index, code, count, on_time, off_time):
        return self._binary(index, code, operate=True)

    def _analog(self, index: int, value: float, operate: bool) -> CommandResult:
        address = (PointKind.ANALOG_OUTPUT, index)
        if self.served.mux.handles(address):
            return self.served.mux.write_from_master(address, float(value)) if operate else CommandResult.success()
        spec, refusal = self._check(PointKind.ANALOG_OUTPUT, index)
        if refusal is not None:
            return refusal
        if operate:
            self.served.store(spec, float(value), from_master=True)
        return CommandResult.success()

    def select_analog_output(self, index, value):
        return self._analog(index, value, operate=False)

    def operate_analog_output(self, index, value, select_sequence):
        return self._analog(index, value, operate=True)

    def direct_operate_analog_output(self, index, value):
        return self._analog(index, value, operate=True)


class ServedOutstation:
    """A dnp3py outstation on ``host:port`` serving the registered points to one master."""

    def __init__(self, host: str, port: int, *, outstation_address: int = 1, master_address: int = 2,
                 unsolicited: bool = False, unsolicited_classes: Iterable[int] = (1, 2, 3),
                 push: PushCallback | None = None, remote_id=None):
        self.host, self.port = host, port
        self.outstation_address, self.master_address = outstation_address, master_address
        self.unsolicited, self.unsolicited_classes = bool(unsolicited), {int(c) for c in unsolicited_classes}
        self.push = push
        self.remote_id = remote_id
        self.points: dict[str, ServedPoint] = {}
        self._by_key: dict[tuple[PointKind, int], ServedPoint] = {}
        self.seeded = 0
        self._synthetic: dict[Address, PointSpec] = {}     # wire points no row declares: buffers, read-backs, flags
        self.mux = Multiplexer(write_wire=self._write_wire, read_wire=self._read_wire, push=self._push_sync, engineering=self._engineering_at)
        self.database = Database(config=DatabaseConfig(
            max_binary_inputs=MAX_POINTS_PER_KIND, max_binary_outputs=MAX_POINTS_PER_KIND,
            max_analog_inputs=MAX_POINTS_PER_KIND, max_analog_outputs=MAX_POINTS_PER_KIND,
            max_counters=MAX_POINTS_PER_KIND, max_frozen_counters=MAX_POINTS_PER_KIND))
        classes = self.unsolicited_classes
        self.outstation = Outstation(
            config=OutstationConfig(address=outstation_address, master_address=master_address,
                                    unsolicited=UnsolicitedConfig(enabled=self.unsolicited, class_1_enabled=1 in classes,
                                                                  class_2_enabled=2 in classes, class_3_enabled=3 in classes)),
            database=self.database, handler=ServedCommandHandler(self))
        self.runner = OutstationTcpRunner(outstation=self.outstation, host=host, port=port)
        self._task: asyncio.Task | None = None

    # ---- configuration ------------------------------------------------------------------------------------------
    def configure_points(self, specs: Iterable[dict], values: dict[str, Any] | None = None, mux_areas: dict | None = None) -> int:
        """Replace the point table, keeping the served values of points that stay. A bad row rejects the whole table.

        A new point starts from the caller's current value for it in ``values`` (the platform's equipment tree, sent
        with the registration, so a proxy that restarted takes the served values back from the platform's memory),
        else from the row's ``initial_value``, else from the kind's zero with quality RESTART, which is how an
        outstation reports a value nobody has set yet. Values this process already holds win over both. The number of
        points seeded from ``values`` is left in ``seeded``.
        """
        values = values or {}
        specs = list(specs)
        # Store rows (a numeric mux_slot) are storage behind an edit buffer, not wire points of their own; the selector
        # rows are wire points that also drive an area. The buffer points come from the store rows' addresses.
        store_specs = [spec for spec in specs if str(spec.get('mux_slot', '') or '').strip() not in ('', SELECTOR)]
        wire_specs = [spec for spec in specs if spec not in store_specs]
        points = [ServedPoint.from_dict(spec) for spec in wire_specs]
        selectors = []
        for spec, point in zip(wire_specs, points):
            if str(spec.get('mux_slot', '') or '').strip() == SELECTOR:
                selectors.append((str(spec['mux_area']), point.key, point.readback, point.topic))
        needed = self.mux.configure(store_specs, selectors, mux_areas)
        self.seeded = 0
        by_key: dict[tuple[PointKind, int], ServedPoint] = {}
        for point in points:
            if point.key in by_key:
                raise ValueError(f'{point.topic} and {by_key[point.key].topic} both name {point.kind.value} {point.index}')
            by_key[point.key] = point
        for point in points:
            if point.readback:
                needed.add(point.readback)
        # Every wire point the areas and the read-backs need that no row declares, as a served point without a topic.
        self._synthetic = {}
        for address in needed:
            if address in by_key:
                continue
            kind, index = address
            spec = ServedPoint.from_dict({'topic': f'<{kind.value} {index}>', 'group': {PointKind.BINARY_INPUT: 1, PointKind.BINARY_OUTPUT: 10,
                                                                                         PointKind.ANALOG_INPUT: 30, PointKind.ANALOG_OUTPUT: 40,
                                                                                         PointKind.COUNTER: 20}[kind], 'index': index,
                                          'remote_writable': kind in OUTPUT_KINDS})
            self._synthetic[address] = spec
            by_key[address] = spec
            points.append(spec)
        for kind, held in KINDS.items():
            store = getattr(self.database, held.store)
            for index in [i for i in store if (kind, i) not in by_key]:
                del store[index]
            for point in points:
                if point.kind is not kind:
                    continue
                # dnp3py raises no events for analog outputs (group 42 is not implemented), so their class is NONE.
                event_class = EventClass.NONE if kind is PointKind.ANALOG_OUTPUT else EventClass(point.event_class)
                config = held.config(event_class=event_class)
                if point.index in store:
                    store[point.index].config = config
                    continue
                initial = point.initial_value
                if point.topic in values and held.updater is not None:
                    try:
                        initial = point.raw_typed(values[point.topic])
                        self.seeded += 1
                    except (TypeError, ValueError) as e:
                        _log.warning(f'{self.host}:{self.port}: ignoring seed value {values[point.topic]!r} for'
                                     f' {point.topic}: {e}')
                        initial = None if initial in (None, '') else point.raw_typed(initial)
                elif initial not in (None, ''):
                    initial = point.raw_typed(initial)
                else:
                    initial = None
                if initial is None:
                    getattr(self.database, held.adder)(point.index, config)
                else:
                    getattr(self.database, held.adder)(point.index, config, value=initial, quality=held.quality.ONLINE)
        self.points = {p.topic: p for p in points if p.key not in self._synthetic}
        self._by_key = by_key
        self.seeded += self.mux.seed(values)
        for schedule in self.mux.schedules.values():
            schedule.start()
        self.mux.refresh_flag()
        return len(self.points) + len(self.mux.by_topic)

    def spec_at(self, kind: PointKind, index: int) -> ServedPoint | None:
        return self._by_key.get((kind, index))

    def describe(self) -> dict:
        return {'host': self.host, 'port': self.port, 'master_address': self.master_address,
                'outstation_address': self.outstation_address, 'points': len(self.points) + len(self.mux.by_topic), 'role': 'outstation',
                'listening': self.runner.is_running, 'areas': self.mux.describe()}

    # ---- the multiplexer's view of the database ------------------------------------------------------------------
    def _write_wire(self, address: Address, raw_value: Any, online: bool) -> None:
        kind, index = address
        held = KINDS[kind]
        if held.updater is None or index not in getattr(self.database, held.store):
            return
        value = raw_value if raw_value is not None else (False if held.python_type is bool else 0)
        if held.python_type is bool:
            value = _as_bool(value)
        elif held.python_type is int:
            value = int(round(float(value)))
        else:
            value = float(value)
        getattr(self.database, held.updater)(index, value, quality=held.quality.ONLINE if online else held.quality.RESTART)

    def _read_wire(self, address: Address) -> Any:
        kind, index = address
        point = getattr(self.database, KINDS[kind].store).get(index)
        if point is None or not (int(point.quality) & 0x01):
            return None
        return point.value

    def _engineering_at(self, address: Address, raw_value: Any) -> Any:
        # A buffer address is served by a synthetic point without scaling; the store rows carry the scaling, so they win.
        area = self.mux.by_buffer.get(address)
        row = next((r for r in area.rows.values() if r.written == address), None) if area else None
        spec = row.spec if row else self._by_key.get(address)
        if spec is None:
            return raw_value
        return spec.engineering(SimpleNamespace(value=raw_value)) if not hasattr(raw_value, 'value') else spec.engineering(raw_value)

    def _push_sync(self, values: dict[str, Any]) -> None:
        if self.push is not None and values:
            asyncio.get_running_loop().create_task(self._push_safely(values))

    # ---- lifecycle ----------------------------------------------------------------------------------------------
    async def start(self, timeout: float = BIND_TIMEOUT):
        """Open the listener (idempotent). Raises what the bind raised, e.g. an address already in use."""
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.get_running_loop().create_task(self.runner.run())
        deadline = asyncio.get_running_loop().time() + timeout
        while not self.runner.is_running:
            if self._task.done():
                self._task.result()          # raises the bind error
                raise RuntimeError(f'outstation listener on {self.host}:{self.port} stopped before it was ready')
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(f'outstation listener on {self.host}:{self.port} did not bind within {timeout} s')
            await asyncio.sleep(0.02)
        _log.info(f'Serving DNP3 outstation {self.outstation_address} to master {self.master_address} on'
                  f' {self.host}:{self.port}.')

    async def close(self):
        await self.runner.stop()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    # ---- values -------------------------------------------------------------------------------------------------
    def _point(self, spec: ServedPoint):
        return getattr(self.database, spec.held.store).get(spec.index)

    def _entry(self, spec: ServedPoint) -> dict | None:
        point = self._point(spec)
        if point is None:
            return None
        quality = int(point.quality)
        timestamp = getattr(point, 'timestamp', None)
        return {'value': spec.engineering(point), 'quality': quality, 'online': bool(quality & 0x01),
                'timestamp': timestamp.to_datetime() if timestamp is not None else None}

    def store(self, spec: ServedPoint, raw_value, from_master: bool = False):
        """Set a served value (raw, database-side); raises events for event-class points. A master's write is pushed."""
        updater = spec.held.updater
        if updater is None:
            raise ValueError(f'{spec.topic}: frozen counters are frozen from their counters, not written')
        getattr(self.database, updater)(spec.index, raw_value, quality=spec.held.quality.ONLINE)
        if spec.readback is not None:
            self._write_wire(spec.readback, raw_value, True)          # the read-back rule: the input follows the output
        if spec.key in self.mux.reference_points():
            self.mux.refresh_flag()
        if from_master and self.push is not None:
            asyncio.get_running_loop().create_task(self._push_safely({spec.topic: spec.engineering(self._point(spec))}))

    async def _push_safely(self, values: dict[str, Any]):
        try:
            await self.push(values)
        except Exception as e:    # pragma: no cover - a failed push must not break the exchange that produced it
            _log.warning(f'{self.host}:{self.port}: unable to push served values: {e}')

    async def push_all(self):
        """Push every served value that has been set (quality ONLINE): the table the platform should hold."""
        values = {}
        for spec in self.points.values():
            entry = self._entry(spec)
            if entry is not None and entry['online']:
                values[spec.topic] = entry['value']
        values.update(self.mux.values())
        if values and self.push is not None:
            await self._push_safely(values)
        return values

    async def read(self, topics: Iterable[str], **_) -> tuple[dict[str, dict], dict[str, str]]:
        """The current served values of ``topics`` in the master role's reply shape."""
        results, errors = {}, {}
        for topic in topics:
            found = self.mux.by_topic.get(topic)
            if found is not None:
                area, row = found
                raw = area.stores[row.slot].get(row.written)
                results[topic] = {'value': self._engineering_at(row.written, raw) if raw is not None else 0, 'quality': 1 if raw is not None else 2,
                                  'online': raw is not None, 'timestamp': None}
                continue
            spec = self.points.get(topic)
            if spec is None:
                errors[topic] = 'unregistered topic'
                continue
            entry = self._entry(spec)
            if entry is None:
                errors[topic] = 'not held by the outstation'
            else:
                results[topic] = entry
        return results, errors

    async def write(self, operations: Iterable[dict], **_) -> tuple[dict[str, dict], dict[str, str]]:
        """Store ``{'topic', 'value'}`` operations as served values (the platform's writes)."""
        results, errors = {}, {}
        for op in operations:
            topic, value = op.get('topic'), op.get('value')
            if topic in self.mux.by_topic:
                ok, message = self.mux.write_from_platform(topic, value)
                if ok:
                    results[topic] = {'status': 'SUCCESS', 'value': value}
                else:
                    errors[topic] = message
                continue
            spec = self.points.get(topic)
            if spec is None:
                errors[str(topic)] = 'unregistered topic'
                continue
            try:
                self.store(spec, spec.raw_typed(value))
            except (TypeError, ValueError) as e:
                errors[topic] = f'bad value {value!r}: {e}'
                continue
            results[topic] = {'status': 'SUCCESS', 'value': value}
        return results, errors


def _as_bool(value) -> bool:
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ('true', 't', 'on', 'yes', 'y', '1'):
            return True
        if text in ('false', 'f', 'off', 'no', 'n', '0', ''):
            return False
        raise ValueError(f'{value!r} is not a boolean')
    return bool(value)
