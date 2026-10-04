"""One DNP3 outstation as the proxy sees it: a dnp3py master and TCP runner, the registered points, and the reads,
writes and unsolicited listening done on their behalf.

Requests to one outstation share one connection and run one at a time. The connection is opened on first use and
reopened on the next request after a link failure; with unsolicited reporting enabled a background listener keeps the
link up and forwards events as they arrive.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

from dnp3.application.builder import build_read_request
from dnp3.application.fragment import ObjectBlock
from dnp3.application.qualifiers import ObjectHeader, RangeCode
from dnp3.core.enums import CommandStatus, ControlCode
from dnp3.master import CommandBuilder, Master, MasterConfig, MasterTcpRunner
from dnp3.master.handler import CommandResponse
from dnp3.master.tcp_runner import LinkError, LinkResetPolicy, MasterRunnerError, ResponseTimeoutError
from dnp3.objects.layout import PointKind, layout_for

from .cache import PointCache, plain_value

_log = logging.getLogger(__name__)

READ_MODES = ('class', 'integrity', 'points')
CONTROL_MODES = ('direct', 'sbo')
#: Variation to assume for a registered point whose registry row names none.
DEFAULT_VARIATIONS = {1: 2, 2: 2, 3: 2, 4: 2, 10: 2, 11: 2, 20: 1, 21: 1, 22: 1, 30: 6, 32: 6, 40: 4, 42: 6}
#: g60 variation for each data class (A.26).
CLASS_VARIATIONS = {0: 1, 1: 2, 2: 3, 3: 4}
#: An analog output status variation (g40) and the command variation (g41) carrying the same encoding.
COMMAND_VARIATIONS = {1: 1, 2: 2, 3: 3, 4: 4}
SCALED_KINDS = frozenset({PointKind.ANALOG_INPUT, PointKind.ANALOG_OUTPUT, PointKind.COUNTER, PointKind.FROZEN_COUNTER})
OUTPUT_KINDS = {PointKind.BINARY_OUTPUT: 12, PointKind.ANALOG_OUTPUT: 41}
QUALITY_ONLINE = 0x01

PushCallback = Callable[[dict[str, Any]], Awaitable[None]]


@dataclass(frozen=True)
class PointSpec:
    """A registered point: where it lives on the outstation and how the driver wants it handled."""
    topic: str
    group: int
    index: int
    variation: int
    kind: PointKind
    scaling: float = 1.0
    control_code: str = 'latch'
    count: int = 1
    on_time: int = 0
    off_time: int = 0

    @classmethod
    def from_dict(cls, spec: dict) -> 'PointSpec':
        group, index = int(spec['group']), int(spec['index'])
        variation = spec.get('variation')
        variation = DEFAULT_VARIATIONS.get(group, 1) if variation in (None, '', 0) else int(variation)
        layout = layout_for(group, variation)
        if layout is None or not PointCache.supports(layout.point_kind):
            raise ValueError(f"{spec.get('topic')}: group {group} variation {variation} is not a point the proxy reads")
        scaling = spec.get('scaling')
        return cls(topic=str(spec['topic']), group=group, index=index, variation=variation, kind=layout.point_kind,
                   scaling=1.0 if scaling in (None, '') else float(scaling),
                   control_code=str(spec.get('control_code') or 'latch').lower(),
                   count=int(spec.get('count') or 1), on_time=int(spec.get('on_time') or 0),
                   off_time=int(spec.get('off_time') or 0))

    @property
    def key(self) -> tuple[PointKind, int]:
        return self.kind, self.index

    def engineering(self, value):
        """Apply the point's scaling to a raw value from the outstation."""
        plain = plain_value(value)
        return plain * self.scaling if self.kind in SCALED_KINDS and self.scaling != 1.0 else plain

    def raw(self, value: float) -> float:
        """The outstation-side value for an engineering value to write."""
        return value / self.scaling if self.scaling not in (0, 1.0) else value


class OutstationClient:
    """A dnp3py master bound to one outstation, serving the proxy's reads, writes and pushes."""

    def __init__(self, host: str, port: int, *, master_address: int = 2, outstation_address: int = 1,
                 response_timeout: float = 5.0, link_reset: bool = True, integrity_poll_interval: float = 3600.0,
                 unsolicited: bool = False, unsolicited_classes: Iterable[int] = (1, 2, 3),
                 push: PushCallback | None = None, listen_slice: float = 0.5, reconnect_delay: float = 5.0,
                 channel=None, remote_id=None):
        self.host, self.port = host, port
        self.cache = PointCache()
        self.master = Master(config=MasterConfig(address=master_address, outstation_address=outstation_address,
                                                 response_timeout=response_timeout), handler=self.cache)
        self.runner = MasterTcpRunner(master=self.master, host=host, port=port, response_timeout=response_timeout,
                                      link_reset=LinkResetPolicy.ON_OPEN if link_reset else LinkResetPolicy.NEVER,
                                      channel=channel)
        self.integrity_poll_interval = float(integrity_poll_interval)
        self.unsolicited = bool(unsolicited)
        self.unsolicited_classes = {int(c) for c in unsolicited_classes}
        self.push = push
        # The caller's identifier for this outstation, carried in pushed messages so the manager routes them to it.
        self.remote_id = remote_id
        self.listen_slice, self.reconnect_delay = listen_slice, reconnect_delay
        self.points: dict[str, PointSpec] = {}
        self._topics_by_key: dict[tuple[PointKind, int], list[str]] = {}
        self._lock = asyncio.Lock()
        self._last_integrity: float | None = None
        self._listener: asyncio.Task | None = None
        self._closing = False

    # ---- configuration ------------------------------------------------------------------------------------------
    def configure_points(self, specs: Iterable[dict]) -> int:
        """Replace the point table. Returns the number of points registered; a bad row rejects the whole table."""
        points = [PointSpec.from_dict(spec) for spec in specs]
        self.points = {p.topic: p for p in points}
        self._topics_by_key = {}
        for p in points:
            self._topics_by_key.setdefault(p.key, []).append(p.topic)
        return len(self.points)

    def describe(self) -> dict:
        return {'host': self.host, 'port': self.port, 'master_address': self.master.config.address,
                'outstation_address': self.master.config.outstation_address, 'points': len(self.points),
                'unsolicited': self.unsolicited, 'open': self.runner.is_open}

    # ---- connection ---------------------------------------------------------------------------------------------
    async def _ensure_open(self):
        if self.runner.is_open:
            return
        try:
            await self.runner.open()
        except Exception as e:
            raise LinkError(f'Unable to connect to outstation {self.host}:{self.port}: {e}') from e
        await self._on_connect()

    async def _on_connect(self):
        """Startup sequence (IEEE 1815-2012 5.1.6): quiet the outstation, take a full snapshot, then enable events."""
        try:
            await self.runner.disable_unsolicited()
        except MasterRunnerError as e:
            _log.debug(f'{self.host}:{self.port}: disable unsolicited not accepted ({e}); continuing')
        await self._integrity_poll()
        if self.unsolicited:
            classes = self.unsolicited_classes
            await self.runner.enable_unsolicited(class_1=1 in classes, class_2=2 in classes, class_3=3 in classes)

    async def _drop_link(self):
        try:
            await self.runner.close()
        except Exception as e:    # pragma: no cover - close() clears state even when the channel objects
            _log.debug(f'{self.host}:{self.port}: error closing link: {e}')

    def start(self):
        """Start the unsolicited listener, when events are wanted. Harmless to call again."""
        if self.unsolicited and (self._listener is None or self._listener.done()):
            self._closing = False
            self._listener = asyncio.get_running_loop().create_task(self._listen_loop())

    async def close(self):
        self._closing = True
        if self._listener is not None:
            self._listener.cancel()
            try:
                await self._listener
            except (asyncio.CancelledError, Exception):
                pass
            self._listener = None
        async with self._lock:
            await self._drop_link()

    # ---- reads --------------------------------------------------------------------------------------------------
    def _integrity_due(self) -> bool:
        if self._last_integrity is None:
            return True
        return self.integrity_poll_interval > 0 and time.monotonic() - self._last_integrity >= self.integrity_poll_interval

    async def _integrity_poll(self):
        await self.runner.integrity_poll()
        self._last_integrity = time.monotonic()

    async def _class_read(self, classes: Iterable[int]):
        blocks = tuple(ObjectBlock(header=ObjectHeader(group=60, variation=CLASS_VARIATIONS[int(c)],
                                                       qualifier=int(RangeCode.ALL_OBJECTS)))
                       for c in sorted({int(c) for c in classes}))
        if not blocks:
            raise ValueError('A class read needs at least one class (0 to 3).')
        await self.runner.request(build_read_request(objects=blocks, seq=self.master.next_request_sequence()))

    async def _range_read(self, specs: Iterable[PointSpec]):
        """One range poll per contiguous index span of each (group, variation)."""
        by_gv: dict[tuple[int, int], set[int]] = {}
        for spec in specs:
            by_gv.setdefault((spec.group, spec.variation), set()).add(spec.index)
        for (group, variation), indexes in by_gv.items():
            for start, stop in _spans(sorted(indexes)):
                await self.runner.request(self.master.build_range_poll(group, variation, start, stop))

    async def read(self, topics: Iterable[str], mode: str = 'class', classes: Iterable[int] = (0,)
                   ) -> tuple[dict[str, dict], dict[str, str]]:
        """Read the outstation as ``mode`` says, then report the named topics from the refreshed cache.

        Returns ``(results, errors)``: results map topic to ``{'value', 'quality', 'online', 'timestamp'}`` with
        scaling applied; errors map a topic to its reason, or the single key ``'link'`` when the exchange failed.
        """
        if mode not in READ_MODES:
            return {}, {'link': f'Unknown read mode {mode!r}; expected one of {READ_MODES}'}
        topics = list(topics)
        results: dict[str, dict] = {}
        errors: dict[str, str] = {}
        specs = []
        for topic in topics:
            spec = self.points.get(topic)
            if spec is None:
                errors[topic] = 'unregistered topic'
            else:
                specs.append(spec)
        async with self._lock:
            try:
                await self._ensure_open()
                if mode == 'integrity' or self._integrity_due():
                    await self._integrity_poll()
                elif mode == 'class':
                    await self._class_read(classes)
                elif specs:
                    await self._range_read(specs)
            except (ResponseTimeoutError, LinkError, MasterRunnerError, ValueError) as e:
                if isinstance(e, LinkError):
                    await self._drop_link()
                return {}, {**errors, 'link': f'{type(e).__name__}: {e}'}
            finally:
                await self._forward_unsolicited()
        for spec in specs:
            value = self.cache.lookup(spec.kind, spec.index)
            if value is None:
                errors[spec.topic] = 'not reported by outstation'
                continue
            quality = getattr(value, 'quality', QUALITY_ONLINE)
            results[spec.topic] = {'value': spec.engineering(value), 'quality': quality,
                                   'online': bool(quality & QUALITY_ONLINE), 'timestamp': getattr(value, 'timestamp', None)}
        return results, errors

    # ---- writes -------------------------------------------------------------------------------------------------
    async def write(self, operations: Iterable[dict], control_mode: str = 'direct'
                    ) -> tuple[dict[str, dict], dict[str, str]]:
        """Operate outputs. ``operations`` are ``{'topic', 'value'}``; one request (two for SBO) carries them all.

        Returns ``(results, errors)``: results map topic to ``{'status': 'SUCCESS', 'value': v}``; errors carry the
        echoed status name or the reason the operation was not sent.
        """
        if control_mode not in CONTROL_MODES:
            return {}, {'link': f'Unknown control mode {control_mode!r}; expected one of {CONTROL_MODES}'}
        builder = CommandBuilder()
        pending: dict[tuple[int, int], tuple[PointSpec, Any]] = {}
        errors: dict[str, str] = {}
        for op in operations:
            topic, value = op.get('topic'), op.get('value')
            spec = self.points.get(topic)
            if spec is None:
                errors[str(topic)] = 'unregistered topic'
                continue
            group = OUTPUT_KINDS.get(spec.kind)
            if group is None:
                errors[topic] = f'{spec.kind.value} is not an output'
                continue
            try:
                if spec.kind is PointKind.BINARY_OUTPUT:
                    builder.add_crob(spec.index, _control_code(spec, _as_bool(value)), spec.count, spec.on_time, spec.off_time)
                else:
                    builder.add_analog(spec.index, spec.raw(float(value)), variation=COMMAND_VARIATIONS.get(spec.variation, 1))
            except (TypeError, ValueError) as e:
                errors[topic] = f'bad value {value!r}: {e}'
                continue
            pending[(group, spec.index)] = (spec, value)
        if not pending:
            return {}, errors
        async with self._lock:
            try:
                await self._ensure_open()
                if control_mode == 'sbo':
                    responses = await self.runner.select_and_operate(builder.build_select())
                else:
                    responses = await self.runner.direct_operate(builder.build_direct_operate())
            except (ResponseTimeoutError, LinkError, MasterRunnerError, ValueError) as e:
                if isinstance(e, LinkError):
                    await self._drop_link()
                return {}, {**errors, **{spec.topic: f'{type(e).__name__}: {e}' for spec, _ in pending.values()}}
            finally:
                await self._forward_unsolicited()
        return _write_results(pending, responses, errors)

    # ---- unsolicited --------------------------------------------------------------------------------------------
    async def _forward_unsolicited(self):
        """Push unsolicited deliveries gathered during the last exchange or listen slice to the driver."""
        pending = self.cache.pop_unsolicited()
        if not pending or self.push is None:
            return
        values: dict[str, Any] = {}
        for kind, index, value in pending:
            for topic in self._topics_by_key.get((kind, index), ()):
                values[topic] = self.points[topic].engineering(value)
        if values:
            try:
                await self.push(values)
            except Exception as e:    # pragma: no cover - a failed push must not break the exchange that produced it
                _log.warning(f'{self.host}:{self.port}: unable to push unsolicited values: {e}')

    async def _listen_loop(self):
        """Keep the link up and listen for unsolicited responses between the proxy's own requests."""
        while not self._closing:
            try:
                async with self._lock:
                    await self._ensure_open()
                    try:
                        await self.runner.listen_unsolicited(timeout=self.listen_slice)
                    finally:
                        await self._forward_unsolicited()
                await asyncio.sleep(0)    # let a waiting read or write take the lock
            except asyncio.CancelledError:
                raise
            except (LinkError, MasterRunnerError, OSError) as e:
                _log.warning(f'{self.host}:{self.port}: link lost while listening ({e}); retrying in {self.reconnect_delay}s')
                await self._drop_link()
                await asyncio.sleep(self.reconnect_delay)
            except Exception as e:    # pragma: no cover
                _log.exception(f'{self.host}:{self.port}: unexpected error in the unsolicited listener: {e}')
                await asyncio.sleep(self.reconnect_delay)


def _spans(indexes: list[int]) -> list[tuple[int, int]]:
    """Contiguous (start, stop) spans of a sorted index list."""
    spans: list[tuple[int, int]] = []
    for index in indexes:
        if spans and index == spans[-1][1] + 1:
            spans[-1] = (spans[-1][0], index)
        else:
            spans.append((index, index))
    return spans


def _as_bool(value) -> bool:
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ('true', 't', 'on', 'yes', 'y', '1', 'latch_on', 'close'):
            return True
        if text in ('false', 'f', 'off', 'no', 'n', '0', 'latch_off', 'trip', ''):
            return False
        raise ValueError(f'{value!r} is not a boolean')
    return bool(value)


def _control_code(spec: PointSpec, on: bool) -> ControlCode:
    if spec.control_code == 'pulse':
        return ControlCode.PULSE_ON if on else ControlCode.PULSE_OFF
    if spec.control_code == 'latch':
        return ControlCode.LATCH_ON if on else ControlCode.LATCH_OFF
    raise ValueError(f"control code {spec.control_code!r} is not 'latch' or 'pulse'")


def _write_results(pending: dict[tuple[int, int], tuple[PointSpec, Any]], responses: list[CommandResponse],
                   errors: dict[str, str]) -> tuple[dict[str, dict], dict[str, str]]:
    results: dict[str, dict] = {}
    echoed = {(r.group, r.index): r.status for r in responses}
    for key, (spec, value) in pending.items():
        status = echoed.get(key)
        if status is None:
            errors[spec.topic] = 'no status echoed by outstation'
        elif status is CommandStatus.SUCCESS:
            results[spec.topic] = {'status': status.name, 'value': value}
        else:
            errors[spec.topic] = f'status {status.name}'
    return results, errors
