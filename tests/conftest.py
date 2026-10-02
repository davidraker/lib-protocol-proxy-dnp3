"""Shared fixtures: a dnp3py Outstation served on the loopback interface, and the point table that names its points.

No network beyond 127.0.0.1 and no hardware. The outstation accepts every binary control and analog values below 1000,
records what it operated, and tracks analog output commands into its database so writes read back.
"""
import asyncio
import contextlib
from dataclasses import dataclass

import pytest

from dnp3.core.enums import ControlCode
from dnp3.core.flags import AnalogQuality, BinaryQuality, CounterQuality
from dnp3.database import (AnalogInputConfig, AnalogOutputConfig, BinaryInputConfig, BinaryOutputConfig, CounterConfig,
                           Database, EventClass)
from dnp3.outstation import CommandResult, DefaultCommandHandler, Outstation, OutstationConfig, OutstationTcpRunner

from protocol_proxy.protocol.dnp3.client import OutstationClient

MASTER, OUTSTATION = 2, 1
ANALOG_LIMIT = 1000
BIND_TIMEOUT = 5.0

POINTS = [
    {'topic': 'd/bi0', 'group': 1, 'variation': 2, 'index': 0},
    {'topic': 'd/bi1', 'group': 1, 'variation': 2, 'index': 1},
    {'topic': 'd/bo0', 'group': 10, 'variation': 2, 'index': 0},
    {'topic': 'd/bo1', 'group': 10, 'variation': 2, 'index': 1, 'control_code': 'pulse', 'on_time': 100, 'count': 2},
    {'topic': 'd/ai0', 'group': 30, 'variation': 6, 'index': 0},
    {'topic': 'd/ai1', 'group': 30, 'variation': 6, 'index': 1, 'scaling': 0.1},
    {'topic': 'd/ai2', 'group': 30, 'variation': '', 'index': 2},             # blank variation: the default
    {'topic': 'd/ao0', 'group': 40, 'variation': 1, 'index': 0, 'scaling': 10},   # INT32 commands, scaled
    {'topic': 'd/ao1', 'group': 40, 'variation': 3, 'index': 1},               # FLT32 commands
    {'topic': 'd/ctr0', 'group': 20, 'variation': 1, 'index': 0},
]


class AcceptingHandler(DefaultCommandHandler):
    """Accepts binary controls and analog values below ANALOG_LIMIT; records what was operated.

    Binary output states are written to the database so a latch reads back (the outstation tracks analog output
    commands itself, AnalogOutputConfig.track_commands).
    """

    def __init__(self, database: Database | None = None):
        self.operated: list[tuple[str, int, object]] = []
        self.database = database

    def _binary(self, index, code):
        self.operated.append(('binary', index, code))
        if self.database is not None and code in (ControlCode.LATCH_ON, ControlCode.LATCH_OFF):
            self.database.update_binary_output(index, code == ControlCode.LATCH_ON)
        return CommandResult.success()

    def select_binary_output(self, index, code, count, on_time, off_time):
        return CommandResult.success()

    def operate_binary_output(self, index, code, count, on_time, off_time, select_sequence):
        return self._binary(index, code)

    def direct_operate_binary_output(self, index, code, count, on_time, off_time):
        return self._binary(index, code)

    def _analog(self, value):
        return CommandResult.success() if value < ANALOG_LIMIT else CommandResult.out_of_range()

    def select_analog_output(self, index, value):
        return self._analog(value)

    def operate_analog_output(self, index, value, select_sequence):
        self.operated.append(('analog', index, value))
        return self._analog(value)

    def direct_operate_analog_output(self, index, value):
        self.operated.append(('analog', index, value))
        return self._analog(value)


def make_database() -> Database:
    db = Database()
    for i in (0, 1):
        db.add_binary_input(i, BinaryInputConfig(event_class=EventClass.CLASS_1))
        db.add_binary_output(i, BinaryOutputConfig())
        db.add_analog_output(i, AnalogOutputConfig())
    for i in (0, 1, 2):
        db.add_analog_input(i, AnalogInputConfig(event_class=EventClass.CLASS_1))
    db.add_counter(0, CounterConfig())
    db.update_binary_input(0, True, quality=BinaryQuality.ONLINE)
    db.update_binary_input(1, False, quality=BinaryQuality.ONLINE)
    for i in (0, 1):
        db.update_binary_output(i, False, quality=BinaryQuality.ONLINE)
    db.update_analog_input(0, 2401, quality=AnalogQuality.ONLINE)
    db.update_analog_input(1, -155, quality=AnalogQuality.ONLINE)        # scaled by 0.1 in the point table: -15.5
    db.update_analog_input(2, 7, quality=AnalogQuality.ONLINE)
    db.update_analog_output(0, 10, quality=AnalogQuality.ONLINE)
    db.update_analog_output(1, 20, quality=AnalogQuality.ONLINE)
    db.update_counter(0, 42, quality=CounterQuality.ONLINE)
    return db


@dataclass
class Served:
    host: str
    port: int
    database: Database
    handler: AcceptingHandler
    runner: OutstationTcpRunner
    task: asyncio.Task


async def serve(database: Database, handler) -> Served:
    # dnp3py's outstation serves analog values as integers (g30v1, g40v1/v2), so the test values are whole numbers
    # and fractional engineering values come from the proxy's scaling.
    outstation = Outstation(config=OutstationConfig(address=OUTSTATION, master_address=MASTER), database=database,
                            handler=handler)
    runner = OutstationTcpRunner(outstation=outstation, host='127.0.0.1', port=0)
    task = asyncio.create_task(runner.run())
    loop = asyncio.get_running_loop()
    deadline = loop.time() + BIND_TIMEOUT
    while not (runner.is_running and runner.local_address is not None):
        if loop.time() > deadline:
            pytest.fail('outstation did not bind in time')
        await asyncio.sleep(0.02)
    host, port = runner.local_address
    return Served(host, port, database, handler, runner, task)


async def stop(served: Served):
    await served.runner.stop()
    served.task.cancel()
    with contextlib.suppress(asyncio.CancelledError, TimeoutError):
        await asyncio.wait_for(served.task, timeout=BIND_TIMEOUT)


@pytest.fixture
async def served():
    database = make_database()
    s = await serve(database, AcceptingHandler(database))
    try:
        yield s
    finally:
        await stop(s)


def make_client(served: Served, points=POINTS, **kwargs) -> OutstationClient:
    client = OutstationClient(served.host, served.port, master_address=MASTER, outstation_address=OUTSTATION,
                              response_timeout=3.0, **kwargs)
    client.configure_points(points)
    return client
