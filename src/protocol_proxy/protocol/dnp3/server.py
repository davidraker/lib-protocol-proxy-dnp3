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
from typing import Any, Iterable

from dnp3.core.enums import ControlCode
from dnp3.core.flags import AnalogQuality, BinaryQuality, CounterQuality
from dnp3.database import (AnalogInputConfig, AnalogOutputConfig, BinaryInputConfig, BinaryOutputConfig, CounterConfig,
                           Database, DatabaseConfig, EventClass)
from dnp3.objects.layout import PointKind
from dnp3.outstation import (CommandResult, DefaultCommandHandler, Outstation, OutstationConfig, OutstationTcpRunner,
                             UnsolicitedConfig)

from .client import OUTPUT_KINDS, PointSpec, PushCallback

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
        return cls(**{f.name: getattr(base, f.name) for f in fields(base)}, event_class=event_class,
                   remote_writable=remote_writable, initial_value=spec.get('initial_value'))

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
        spec, refusal = self._check(PointKind.BINARY_OUTPUT, index)
        if refusal is not None:
            return refusal
        if code not in ON_CODES and code not in OFF_CODES:
            return CommandResult.not_supported(f'control code {code!r} not supported')
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
    def configure_points(self, specs: Iterable[dict]) -> int:
        """Replace the point table, keeping the served values of points that stay. A bad row rejects the whole table.

        New points start from their ``initial_value`` when the row gives one (quality ONLINE), otherwise from the
        kind's zero with quality RESTART, which is how an outstation reports a value nobody has set yet.
        """
        points = [ServedPoint.from_dict(spec) for spec in specs]
        by_key: dict[tuple[PointKind, int], ServedPoint] = {}
        for point in points:
            if point.key in by_key:
                raise ValueError(f'{point.topic} and {by_key[point.key].topic} both name {point.kind.value} {point.index}')
            by_key[point.key] = point
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
                elif point.initial_value in (None, ''):
                    getattr(self.database, held.adder)(point.index, config)
                else:
                    getattr(self.database, held.adder)(point.index, config, value=point.raw_typed(point.initial_value),
                                                       quality=held.quality.ONLINE)
        self.points = {p.topic: p for p in points}
        self._by_key = by_key
        return len(self.points)

    def spec_at(self, kind: PointKind, index: int) -> ServedPoint | None:
        return self._by_key.get((kind, index))

    def describe(self) -> dict:
        return {'host': self.host, 'port': self.port, 'master_address': self.master_address,
                'outstation_address': self.outstation_address, 'points': len(self.points), 'role': 'outstation',
                'listening': self.runner.is_running}

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
        if values and self.push is not None:
            await self._push_safely(values)
        return values

    async def read(self, topics: Iterable[str], **_) -> tuple[dict[str, dict], dict[str, str]]:
        """The current served values of ``topics`` in the master role's reply shape."""
        results, errors = {}, {}
        for topic in topics:
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
