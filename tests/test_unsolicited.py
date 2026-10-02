"""The unsolicited listener, over dnp3py's in-memory channel pair with a hand-rolled outstation peer.

dnp3py's TCP outstation runner does not yet send unsolicited responses on its own, so the peer here answers the
client's startup requests and then emits an unsolicited fragment, the way a conformant outstation reports an event.
"""
import asyncio
import struct

import pytest

from dnp3.application.builder import build_response, build_unsolicited_response
from dnp3.application.fragment import ObjectBlock
from dnp3.application.qualifiers import ObjectHeader
from dnp3.core.enums import FunctionCode
from dnp3.datalink.builder import build_unconfirmed_user_data
from dnp3.datalink.parser import FrameParser
from dnp3.transport.segment import TransportSegment
from dnp3.transport_io.simulator import create_channel_pair

from protocol_proxy.protocol.dnp3.client import OutstationClient

MASTER, OUTSTATION = 2, 1


def analog_block(index: int, value: float) -> ObjectBlock:
    """One g30v5 analog input with a 1-byte index prefix."""
    return ObjectBlock(header=ObjectHeader(group=30, variation=5, qualifier=0x17),
                       data=bytes([1, index, 0x01]) + struct.pack('<f', value))


class Peer:
    """Frames application fragments onto the far channel and reads what the master sends."""

    def __init__(self, channel):
        self.channel = channel
        self.parser = FrameParser()
        self.requests: list[bytes] = []

    async def send(self, app_bytes: bytes):
        segment = TransportSegment.build(fir=True, fin=True, seq=0, payload=app_bytes)
        frame = build_unconfirmed_user_data(destination=MASTER, source=OUTSTATION, dir_from_master=False,
                                            user_data=segment.to_bytes())
        await self.channel.write_all(frame.to_bytes())

    async def read_fragment(self, timeout=2.0) -> bytes:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            data = await asyncio.wait_for(self.channel.read(4096), timeout=deadline - asyncio.get_running_loop().time())
            for frame in self.parser.feed(data):
                if frame.user_data:
                    payload = TransportSegment.from_bytes(frame.user_data).payload
                    self.requests.append(payload)
                    return payload

    async def answer_startup(self):
        """DISABLE_UNSOLICITED, the integrity poll and ENABLE_UNSOLICITED each get a response."""
        for _ in range(3):
            request = await self.read_fragment()
            seq, function = request[0] & 0x0F, request[1]
            objects = (analog_block(0, 10.0),) if function == FunctionCode.READ.value else ()
            await self.send(build_response(objects=objects, seq=seq).to_bytes())


async def test_listener_forwards_unsolicited_values_scaled_and_confirms_them():
    channel_a, channel_b = create_channel_pair()
    await channel_a.open()
    await channel_b.open()
    pushed: list[dict] = []

    async def push(values):
        pushed.append(values)

    client = OutstationClient('sim', 0, master_address=MASTER, outstation_address=OUTSTATION, link_reset=False,
                              unsolicited=True, listen_slice=0.1, push=push, channel=channel_a, response_timeout=2.0)
    client.configure_points([{'topic': 'd/ai0', 'group': 30, 'variation': 6, 'index': 0, 'scaling': 0.1},
                             {'topic': 'd/ai7', 'group': 30, 'variation': 6, 'index': 7}])
    peer = Peer(channel_b)
    client.start()
    await peer.answer_startup()
    assert [r[1] for r in peer.requests] == [FunctionCode.DISABLE_UNSOLICITED.value, FunctionCode.READ.value,
                                             FunctionCode.ENABLE_UNSOLICITED.value]
    assert pushed == []                                            # the startup poll is solicited

    await asyncio.sleep(0.15)                                      # the listener is now in a listen slice
    unsolicited = build_unsolicited_response(objects=(analog_block(0, 55.0), analog_block(3, 1.0)), seq=5, con=True)
    await peer.send(unsolicited.to_bytes())
    confirm = await peer.read_fragment()
    assert confirm[1] == FunctionCode.CONFIRM.value and confirm[0] & 0x10   # UNS bit echoed on the CONFIRM
    for _ in range(50):
        if pushed:
            break
        await asyncio.sleep(0.02)
    assert pushed == [{'d/ai0': pytest.approx(5.5)}]               # index 3 is not registered: not forwarded

    # A read between listen slices still completes, and the cache carries the unsolicited value.
    async def answer_read():
        request = await peer.read_fragment()
        await peer.send(build_response(objects=(analog_block(7, 4.0),), seq=request[0] & 0x0F).to_bytes())
    answering = asyncio.create_task(answer_read())
    results, errors = await client.read(['d/ai0', 'd/ai7'], mode='class')
    await answering
    assert errors == {} and results['d/ai0']['value'] == pytest.approx(5.5) and results['d/ai7']['value'] == 4.0
    await client.close()
