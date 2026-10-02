"""The proxy's view of an outstation's points: dnp3py's handler store plus a record of unsolicited deliveries."""
from __future__ import annotations

from typing import Any

from dnp3.master import DefaultSOEHandler
from dnp3.master.handler import AnalogValue, BinaryValue, CommandResponse, CounterValue, ResponseInfo
from dnp3.objects.layout import PointKind

Delivered = tuple[PointKind, int, Any]


class PointCache(DefaultSOEHandler):
    """Keeps the latest value of every point the outstation reported, by kind and index.

    The library's handler already stores each kind; this adds a lookup by ``PointKind`` (what the proxy knows about a
    registered point from its group and variation) and remembers which deliveries arrived unsolicited, so the client
    can push exactly those to the driver after each exchange.
    """

    def __init__(self):
        super().__init__()
        self.unsolicited: list[Delivered] = []

    # ---- deliveries -------------------------------------------------------------------------------------------
    def _note(self, kind: PointKind, values: list, info: ResponseInfo):
        if info.is_unsolicited:
            self.unsolicited.extend((kind, v.index, v) for v in values)

    def on_binary_input(self, values: list[BinaryValue], info: ResponseInfo) -> None:
        super().on_binary_input(values, info)
        self._note(PointKind.BINARY_INPUT, values, info)

    def on_binary_output(self, values: list[BinaryValue], info: ResponseInfo) -> None:
        super().on_binary_output(values, info)
        self._note(PointKind.BINARY_OUTPUT, values, info)

    def on_analog_input(self, values: list[AnalogValue], info: ResponseInfo) -> None:
        super().on_analog_input(values, info)
        self._note(PointKind.ANALOG_INPUT, values, info)

    def on_analog_output(self, values: list[AnalogValue], info: ResponseInfo) -> None:
        super().on_analog_output(values, info)
        self._note(PointKind.ANALOG_OUTPUT, values, info)

    def on_counter(self, values: list[CounterValue], info: ResponseInfo) -> None:
        super().on_counter(values, info)
        self._note(PointKind.COUNTER, values, info)

    def on_frozen_counter(self, values: list[CounterValue], info: ResponseInfo) -> None:
        super().on_frozen_counter(values, info)
        self._note(PointKind.FROZEN_COUNTER, values, info)

    def on_double_bit_input(self, values, info: ResponseInfo) -> None:
        super().on_double_bit_input(values, info)
        self._note(PointKind.DOUBLE_BIT_INPUT, values, info)

    def on_command_response(self, values: list[CommandResponse], info: ResponseInfo) -> None:
        super().on_command_response(values, info)

    # ---- lookups ----------------------------------------------------------------------------------------------
    _STORES = {
        PointKind.BINARY_INPUT: '_binary_inputs',
        PointKind.BINARY_OUTPUT: '_binary_outputs',
        PointKind.ANALOG_INPUT: '_analog_inputs',
        PointKind.ANALOG_OUTPUT: '_analog_outputs',
        PointKind.COUNTER: '_counters',
        PointKind.FROZEN_COUNTER: '_frozen_counters',
        PointKind.DOUBLE_BIT_INPUT: '_double_bit_inputs',
    }

    @classmethod
    def supports(cls, kind: PointKind) -> bool:
        return kind in cls._STORES

    def lookup(self, kind: PointKind, index: int):
        """The latest value object of a point, or None if the outstation has not reported it."""
        store = self._STORES.get(kind)
        return getattr(self, store).get(index) if store else None

    def pop_unsolicited(self) -> list[Delivered]:
        """Deliveries that arrived unsolicited since the last call, in arrival order."""
        pending, self.unsolicited = self.unsolicited, []
        return pending


def plain_value(value) -> Any:
    """The JSON-friendly value of a dnp3py value object: bool, float, int, or a double-bit state name."""
    state = getattr(value, 'state', None)
    if state is not None:
        return getattr(state, 'name', state)
    return value.value
