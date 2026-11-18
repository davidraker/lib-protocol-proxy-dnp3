"""Multiplexed areas of a served outstation: curves and schedules stored behind one edit buffer each.

IEEE 1815.2 exposes curves and schedules through an edit buffer: a selector point names the stored instance the buffer
shows, the buffer's outputs take the master's writes and their read-back inputs show the instance's contents. The
registry describes the data, not the wire (DNP3_SERVED_CURVE_SCHEDULE_STORAGE_PLAN.md, 3.1): one selector row per
area, one *store row* per buffer value per stored instance (``mux_area``, ``mux_slot``, the written address and the
read-back address), and no rows for the buffer points themselves. This module keeps the stores and derives the wire
buffer from them.

Everything protocol-specific about *which* points mean what (the function points that reference a curve, the
function's enable point, the "referenced by a mode" flag, a schedule area's status and command points) arrives as
addresses in the registration's ``mux_areas`` (written by the service's builder from the standard), so the proxy applies
the rules without knowing the standard's index map:

* selector out of range: ``OUT_OF_RANGE`` (1815.2 6.1.3 recommends it for a curve that does not exist);
* a buffer write with nothing selected: ``OUT_OF_RANGE`` (not addressed by the standard; refused so a write is never
  acknowledged and lost);
* a write into a curve that an enabled function references: ``AUTOMATION_INHIBIT``, the curve unchanged (the lock of
  6.1.3); pairs beyond the number of points are stored as written (6.1.3 c: "unused", not invalid);
* schedules run the state machine of 6.1.4.3 up to Enabled: select (not while Running), submit validates statically
  (offsets non-decreasing, action type and index valid, value within the target's range), enable needs Validated;
  schedule ID 0 deletes.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from dnp3.core.enums import CommandStatus
from dnp3.objects.layout import PointKind
from dnp3.outstation import CommandResult

from .client import PointSpec

_log = logging.getLogger(__name__)

SELECTOR = 'selector'
KIND_OF_GROUP = {1: PointKind.BINARY_INPUT, 10: PointKind.BINARY_OUTPUT, 30: PointKind.ANALOG_INPUT, 40: PointKind.ANALOG_OUTPUT,
                 20: PointKind.COUNTER}
Address = tuple[PointKind, int]

# Schedule states of IEEE 1815.2-2025, 6.1.4.2 c) 1) i).
EDITING, SUBMITTED, VALIDATED, ENABLED, RUNNING, PREEMPTED, CYCLE_COMPLETED, EXPIRED, DISABLED, INVALID, ERROR = range(1, 12)
NOT_IN_USE = 0


def address_of(spec: dict | None) -> Address | None:
    """``{'group', 'index'}`` -> (kind, index); None when absent."""
    if not spec or spec.get('index') in (None, ''):
        return None
    return KIND_OF_GROUP[int(spec['group'])], int(spec['index'])


def out_of_range(message: str) -> CommandResult:
    return CommandResult(status=CommandStatus.OUT_OF_RANGE, message=message)


def automation_inhibit(message: str) -> CommandResult:
    return CommandResult(status=CommandStatus.AUTOMATION_INHIBIT, message=message)


@dataclass
class StoreRow:
    """One value of a stored instance: the platform's point (``topic``), presented on the wire at ``written`` and shown
    back at ``readback`` while its slot is selected."""
    topic: str
    slot: int
    written: Address
    readback: Address | None
    spec: PointSpec                 # scaling and typing of the written point


@dataclass
class Reference:
    """A function point that names a stored instance by number, and the point that enables the function (None: always
    in effect while referencing). A referenced instance is locked while its function is enabled."""
    point: Address
    enable: Address | None = None


@dataclass
class MultiplexedArea:
    label: str
    selector: Address | None = None
    selector_readback: Address | None = None
    selector_topic: str | None = None
    rows: dict[tuple[int, Address], StoreRow] = field(default_factory=dict)      # (slot, written) -> row
    buffer: dict[Address, Address | None] = field(default_factory=dict)          # written -> readback
    stores: dict[int, dict[Address, Any]] = field(default_factory=dict)          # slot -> written -> raw value
    selected: int = 0
    references: list[Reference] = field(default_factory=list)
    referenced_flag: Address | None = None                                        # "Selected Curve is Referenced by a Mode"

    @property
    def slots(self) -> int:
        return max(self.stores) if self.stores else 0

    def add_row(self, row: StoreRow) -> None:
        self.rows[(row.slot, row.written)] = row
        self.buffer[row.written] = row.readback or self.buffer.get(row.written)
        self.stores.setdefault(row.slot, {}).setdefault(row.written, None)
        for slot in self.stores.values():
            slot.setdefault(row.written, None)

    def row_at(self, slot: int, written: Address) -> StoreRow | None:
        return self.rows.get((slot, written))

    def describe(self) -> dict:
        return {'label': self.label, 'slots': self.slots, 'selected': self.selected,
                'buffer_points': len(self.buffer), 'selector': list(self.selector) if self.selector else None}


class Multiplexer:
    """The multiplexed areas of one served outstation and the rules that apply to the master's and the platform's
    writes to them. The outstation hands it the database operations it needs through callbacks, so this module knows
    nothing of dnp3py's database layout."""

    def __init__(self, *, write_wire: Callable[[Address, Any, bool], None], read_wire: Callable[[Address], Any],
                 push: Callable[[dict[str, Any]], None], engineering: Callable[[Address, Any], Any]):
        self.areas: dict[str, MultiplexedArea] = {}
        self.by_selector: dict[Address, MultiplexedArea] = {}
        self.by_buffer: dict[Address, MultiplexedArea] = {}
        self.by_topic: dict[str, tuple[MultiplexedArea, StoreRow]] = {}
        self.schedules: dict[str, 'ScheduleArea'] = {}
        self._write_wire = write_wire       # (address, raw value or None for RESTART, online) -> None
        self._read_wire = read_wire         # address -> raw value or None
        self._push = push                   # {topic: engineering value}
        self._engineering = engineering     # (address, raw) -> engineering, by the wire point's spec

    # ---- configuration ---------------------------------------------------------------------------------------
    def configure(self, store_specs: Iterable[dict], selectors: Iterable[tuple[str, Address, Address | None, str]],
                  area_config: dict | None = None) -> set[Address]:
        """Build the areas from the store rows (``mux_slot`` numeric) and the selector rows (``mux_slot`` selector).
        ``area_config`` is the registration's ``mux_areas`` (references, flags, schedule points). Returns every wire
        address the areas need in the database besides the selectors: the buffer points and their read-backs."""
        self.areas.clear(); self.by_selector.clear(); self.by_buffer.clear(); self.by_topic.clear(); self.schedules.clear()
        for label, address, readback, topic in selectors:
            area = self.areas.setdefault(label, MultiplexedArea(label))
            area.selector, area.selector_readback, area.selector_topic = address, readback, topic
            self.by_selector[address] = area
        for spec in store_specs:
            label = str(spec['mux_area'])
            area = self.areas.setdefault(label, MultiplexedArea(label))
            base = PointSpec.from_dict(spec)
            row = StoreRow(topic=base.topic, slot=int(spec['mux_slot']), written=(base.kind, base.index), readback=address_of(spec.get('readback')), spec=base)
            area.add_row(row)
            self.by_buffer[row.written] = area
            self.by_topic[row.topic] = (area, row)
        for label, config in (area_config or {}).items():
            area = self.areas.get(label)
            if area is None:
                continue
            area.references = [Reference(address_of(r.get('point')), address_of(r.get('enable'))) for r in config.get('references') or [] if address_of(r.get('point'))]
            area.referenced_flag = address_of(config.get('referenced_flag'))
            if config.get('schedule'):
                self.schedules[label] = ScheduleArea(area, config['schedule'], self)
        needed: set[Address] = set()
        for area in self.areas.values():
            for written, readback in area.buffer.items():
                needed.add(written)
                if readback:
                    needed.add(readback)
            if area.referenced_flag:
                needed.add(area.referenced_flag)
            if area.selector_readback:
                needed.add(area.selector_readback)
        for schedule in self.schedules.values():
            needed |= schedule.wire_points()
        return needed

    def seed(self, values: dict[str, Any]) -> int:
        """Store rows' values from the platform's tree (a proxy restart); returns how many were taken."""
        taken = 0
        for topic, value in values.items():
            found = self.by_topic.get(topic)
            if found is None:
                continue
            area, row = found
            try:
                area.stores[row.slot][row.written] = row.spec.raw_typed(value) if hasattr(row.spec, 'raw_typed') else row.spec.raw(float(value))
                taken += 1
            except (TypeError, ValueError) as e:
                _log.warning(f'ignoring seed value {value!r} for {topic}: {e}')
        for area in self.areas.values():
            # the selection survives a restart with the selector's seeded value
            if area.selector_topic and area.selector_topic in values:
                try:
                    slot = int(round(float(values[area.selector_topic])))
                    area.selected = slot if slot in area.stores else 0
                except (TypeError, ValueError):
                    pass
            if area.selected:
                self._show(area)
        return taken

    # ---- the master's writes -----------------------------------------------------------------------------------
    def handles(self, address: Address) -> bool:
        return address in self.by_selector or address in self.by_buffer or any(address in s.commands for s in self.schedules.values())

    def write_from_master(self, address: Address, raw_value: Any) -> CommandResult:
        """A master's OPERATE on a selector, a buffer point or a schedule command; the caller has checked the address."""
        if address in self.by_selector:
            return self._select(self.by_selector[address], raw_value, from_master=True)
        for schedule in self.schedules.values():
            if address in schedule.commands:
                return schedule.command(address, raw_value)
        area = self.by_buffer[address]
        if area.selected == 0:
            return out_of_range(f'{area.label}: nothing selected; write the selector first')
        if area.label in self.schedules and self.schedules[area.label].state(area.selected) == RUNNING:
            return automation_inhibit(f'{area.label} {area.selected} is running')
        if self.is_locked(area, area.selected):
            return automation_inhibit(f'{area.label} {area.selected} is referenced by an enabled function')
        self._store(area, area.selected, address, raw_value, push=True)
        if area.label in self.schedules:
            self.schedules[area.label].edited(area.selected)
        return CommandResult.success()

    def _select(self, area: MultiplexedArea, raw_value: Any, from_master: bool) -> CommandResult:
        try:
            slot = int(round(float(raw_value)))
        except (TypeError, ValueError):
            return out_of_range(f'{area.label}: selector value {raw_value!r} is not a number')
        if slot != 0 and slot not in area.stores:
            return out_of_range(f'{area.label} {slot} does not exist; {area.slots} are stored')
        if area.label in self.schedules and slot and self.schedules[area.label].state(slot) == RUNNING:
            return out_of_range(f'{area.label} {slot} is running and cannot be selected for editing')
        area.selected = slot
        self._write_wire(area.selector, float(slot), True)
        if area.selector_readback:
            self._write_wire(area.selector_readback, float(slot), True)
        if from_master and area.selector_topic:
            self._push({area.selector_topic: float(slot)})
        self._show(area)
        if area.label in self.schedules:
            self.schedules[area.label].selected(slot)
        return CommandResult.success()

    # ---- the platform's writes ---------------------------------------------------------------------------------
    def write_from_platform(self, topic: str, value: Any) -> tuple[bool, str]:
        """A store row written by the platform: filed in its slot without touching the selector. Returns (ok, message)."""
        found = self.by_topic.get(topic)
        if found is None:
            return False, 'not a store row'
        area, row = found
        if self.is_locked(area, row.slot):
            return False, f'{area.label} {row.slot} is referenced by an enabled function'
        try:
            raw = row.spec.raw_typed(value) if hasattr(row.spec, 'raw_typed') else row.spec.raw(float(value))
        except (TypeError, ValueError) as e:
            return False, f'bad value {value!r}: {e}'
        self._store(area, row.slot, row.written, raw, push=False)
        return True, 'SUCCESS'

    # ---- state ---------------------------------------------------------------------------------------------------
    def _store(self, area: MultiplexedArea, slot: int, written: Address, raw_value: Any, push: bool) -> None:
        row = area.row_at(slot, written)
        if row is None:
            return
        raw = row.spec.raw_typed(raw_value) if not isinstance(raw_value, (int, float, bool)) and hasattr(row.spec, 'raw_typed') else raw_value
        area.stores[slot][written] = raw
        if slot == area.selected:
            self._write_wire(written, raw, True)
            if row.readback:
                self._write_wire(row.readback, raw, True)
        if push:
            self._push({row.topic: self._engineering(written, raw)})

    def _show(self, area: MultiplexedArea) -> None:
        """The wire buffer shows the selected store; RESTART where the store has never been written."""
        store = area.stores.get(area.selected, {}) if area.selected else {}
        for written, readback in area.buffer.items():
            raw = store.get(written)
            self._write_wire(written, raw, raw is not None)
            if readback:
                self._write_wire(readback, raw, raw is not None)
        self.refresh_flag(area)

    def referenced_by(self, area: MultiplexedArea, slot: int) -> list[Reference]:
        out = []
        for ref in area.references:
            value = self._read_wire(ref.point)
            try:
                if value is not None and int(round(float(value))) == slot:
                    out.append(ref)
            except (TypeError, ValueError):
                continue
        return out

    def is_locked(self, area: MultiplexedArea, slot: int) -> bool:
        """Referenced by a function that is enabled (or has no enable point, so is always in effect)."""
        for ref in self.referenced_by(area, slot):
            if ref.enable is None or bool(self._read_wire(ref.enable)):
                return True
        return False

    def refresh_flag(self, area: MultiplexedArea | None = None) -> None:
        """The "referenced by a mode" flag follows the selected instance; called after reference points change too."""
        for a in ([area] if area else self.areas.values()):
            if a.referenced_flag:
                self._write_wire(a.referenced_flag, bool(a.selected and self.referenced_by(a, a.selected)), True)

    def reference_points(self) -> set[Address]:
        return {ref.point for a in self.areas.values() for ref in a.references} | {ref.enable for a in self.areas.values() for ref in a.references if ref.enable}

    def values(self) -> dict[str, Any]:
        """Every stored value with a topic, as engineering values: what the platform should hold after a restart."""
        out = {}
        for area in self.areas.values():
            for (slot, written), row in area.rows.items():
                raw = area.stores[slot].get(written)
                if raw is not None:
                    out[row.topic] = self._engineering(written, raw)
        return out

    def describe(self) -> list[dict]:
        out = []
        for label, area in self.areas.items():
            info = area.describe()
            info['instances'] = {slot: {'written': sum(v is not None for v in store.values()), 'referenced_by': len(self.referenced_by(area, slot)),
                                        'locked': self.is_locked(area, slot)} for slot, store in sorted(area.stores.items())}
            if label in self.schedules:
                info['states'] = {slot: self.schedules[label].state(slot) for slot in sorted(area.stores)}
            out.append(info)
        return out


class ScheduleArea:
    """The schedule state machine of IEEE 1815.2-2025 6.1.4.3, up to Enabled, for one schedule area. Running and the
    completion states belong to whatever executes schedules; the proxy keeps the stores and reports the states.

    ``config`` (from the registration's ``mux_areas.<label>.schedule``) names the wire points the standard uses:
    ``submit`` and ``enable`` (BO commands on the selected schedule), ``validated`` and ``enabled`` (BI flags),
    ``status`` (AI, the selected schedule's state), ``id``, ``num_entries`` (buffer written addresses as
    ``{'group', 'index'}``), ``entries`` (``[{'offset', 'type', 'index', 'value'}]`` buffer written addresses per entry),
    ``blocks`` (``{slot: {'status', 'priority', 'step'}}`` the per-schedule status points) and ``count`` (the point that
    announces how many schedules exist). Every key is optional; what is absent is not served."""

    def __init__(self, area: MultiplexedArea, config: dict, mux: Multiplexer):
        self.area, self.mux = area, mux
        self.submit = address_of(config.get('submit'))
        self.enable = address_of(config.get('enable'))
        self.validated_flag = address_of(config.get('validated'))
        self.enabled_flag = address_of(config.get('enabled'))
        self.status = address_of(config.get('status'))
        self.id_point = address_of(config.get('id'))
        self.priority_point = address_of(config.get('priority'))
        self.num_entries = address_of(config.get('num_entries'))
        self.entries = [{k: address_of(v) for k, v in entry.items()} for entry in config.get('entries') or []]
        self.blocks = {int(k): {name: address_of(v) for name, v in block.items()} for k, block in (config.get('blocks') or {}).items()}
        self.count_point = address_of(config.get('count'))
        self.ranges: dict[Address, tuple[float | None, float | None]] = {}
        for entry in config.get('targets') or []:          # the writable points a schedule may act on, with their ranges
            address = address_of(entry)
            if address:
                self.ranges[address] = (entry.get('minimum'), entry.get('maximum'))
        self.states: dict[int, int] = {slot: NOT_IN_USE for slot in area.stores}
        self.commands = {a for a in (self.submit, self.enable) if a}

    def wire_points(self) -> set[Address]:
        points = {a for a in (self.submit, self.enable, self.validated_flag, self.enabled_flag, self.status, self.count_point) if a}
        for block in self.blocks.values():
            points |= {a for a in block.values() if a}
        return points

    def start(self) -> None:
        if self.count_point:
            self.mux._write_wire(self.count_point, float(self.area.slots), True)
        for slot in self.area.stores:
            self._show_block(slot)

    def state(self, slot: int) -> int:
        return self.states.get(slot, NOT_IN_USE)

    def selected(self, slot: int) -> None:
        if slot:
            self._set_state(slot, EDITING)
            self._flags(validated=False, enabled=False)
        else:
            self._flags(validated=False, enabled=False)
            if self.status:
                self.mux._write_wire(self.status, float(NOT_IN_USE), True)

    def edited(self, slot: int) -> None:
        if self.state(slot) in (SUBMITTED, VALIDATED, ENABLED, INVALID):
            self._set_state(slot, EDITING)
            self._flags(validated=False, enabled=False)

    def command(self, address: Address, raw_value: Any) -> CommandResult:
        slot = self.area.selected
        if not slot:
            return out_of_range(f'{self.area.label}: nothing selected')
        on = bool(raw_value)
        if address == self.submit:
            if not on:
                return CommandResult.success()
            store = self.area.stores.get(slot, {})
            if self.id_point and store.get(self.id_point) is not None and float(store[self.id_point]) == 0:
                # 6.1.4.4 b): setting the Schedule ID to 0 deletes the schedule
                for key in store:
                    store[key] = None
                self._set_state(slot, NOT_IN_USE)
                self._flags(validated=False, enabled=False)
                self.mux._show(self.area)
                return CommandResult.success()
            self._set_state(slot, SUBMITTED)
            problems = self.validate(slot)
            if problems:
                self._set_state(slot, INVALID)
                self._flags(validated=False)
                _log.info(f'{self.area.label} {slot} invalid: {"; ".join(problems)}')
            else:
                self._set_state(slot, VALIDATED)
                self._flags(validated=True)
            return CommandResult.success()
        if address == self.enable:
            if not on:
                if self.state(slot) == ENABLED:
                    self._set_state(slot, VALIDATED)
                    self._flags(enabled=False)
                return CommandResult.success()
            if self.state(slot) != VALIDATED:
                # 6.1.4.3 d) 3) i): not validated: the transient Disabled state, then back to Validated (or where it was)
                previous = self.state(slot)
                self._set_state(slot, DISABLED)
                self._set_state(slot, previous if previous != DISABLED else EDITING)
                self._flags(enabled=False)
                return CommandResult.success()
            self._set_state(slot, ENABLED)
            self._flags(enabled=True)
            return CommandResult.success()
        return CommandResult.not_supported('not a schedule command')

    def validate(self, slot: int) -> list[str]:
        """The static checks of 6.1.4.3 c): the schedule's identity, the entry count, non-decreasing offsets, action
        type and index valid, values within the target's range."""
        store = self.area.stores.get(slot, {})
        problems = []
        schedule_id = store.get(self.id_point) if self.id_point else None
        if self.id_point and (schedule_id is None or float(schedule_id) <= 0):
            problems.append('no schedule identity (a positive number; 0 deletes the schedule)')
        count = store.get(self.num_entries) if self.num_entries else None
        try:
            count = int(round(float(count))) if count is not None else len(self.entries)
        except (TypeError, ValueError):
            problems.append(f'number of entries {count!r} is not a number'); count = 0
        if count < 0 or count > len(self.entries):
            problems.append(f'number of entries {count} is not 0 to {len(self.entries)}')
            count = min(max(count, 0), len(self.entries))
        last = None
        for n, entry in enumerate(self.entries[:count], start=1):
            offset = store.get(entry.get('offset')) if entry.get('offset') else None
            if offset is None:
                problems.append(f'entry {n}: no time offset'); continue
            if last is not None and float(offset) < float(last):
                problems.append(f'entry {n}: time offset {offset} before entry {n - 1}\'s {last}')
            last = offset
            kind = store.get(entry.get('type')) if entry.get('type') else None
            index = store.get(entry.get('index')) if entry.get('index') else None
            value = store.get(entry.get('value')) if entry.get('value') else None
            if kind is not None and index is not None:
                try:
                    target_kind = {1: PointKind.ANALOG_OUTPUT, 2: PointKind.BINARY_OUTPUT}.get(int(round(float(kind))))
                except (TypeError, ValueError):
                    target_kind = None
                if target_kind is None:
                    problems.append(f'entry {n}: action type {kind} is not AO (1) or BO (2)'); continue
                target = (target_kind, int(round(float(index))))
                if self.ranges and target not in self.ranges:
                    problems.append(f'entry {n}: {target_kind.value} {target[1]} is not a point this outstation serves'); continue
                low, high = self.ranges.get(target, (None, None))
                if value is not None and ((low is not None and float(value) < float(low)) or (high is not None and float(value) > float(high))):
                    problems.append(f'entry {n}: value {value} outside {low} to {high}')
        return problems

    def _set_state(self, slot: int, state: int) -> None:
        self.states[slot] = state
        if slot == self.area.selected and self.status:
            self.mux._write_wire(self.status, float(state), True)
        self._show_block(slot)

    def _show_block(self, slot: int) -> None:
        block = self.blocks.get(slot)
        if not block:
            return
        if block.get('status'):
            self.mux._write_wire(block['status'], float(self.state(slot)), True)
        if block.get('priority') and self.priority_point:
            priority = self.area.stores.get(slot, {}).get(self.priority_point)
            self.mux._write_wire(block['priority'], priority, priority is not None)
        if block.get('step'):
            self.mux._write_wire(block['step'], 0.0, True)

    def _flags(self, validated: bool | None = None, enabled: bool | None = None) -> None:
        if validated is not None and self.validated_flag:
            self.mux._write_wire(self.validated_flag, validated, True)
        if enabled is not None and self.enabled_flag:
            self.mux._write_wire(self.enabled_flag, enabled, True)
