"""The outstation role: a ServedOutstation on the loopback interface, exercised by the proxy's own master client."""
import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from protocol_proxy.protocol.dnp3.client import OutstationClient
from protocol_proxy.protocol.dnp3.server import ServedOutstation, ServedPoint
from tests.test_proxy import call, make_proxy

SERVED_POINTS = [
    {'topic': 's/ai0', 'group': 30, 'variation': 6, 'index': 0, 'initial_value': 240.1, 'scaling': 0.1},   # held raw 2401
    {'topic': 's/ai1', 'group': 30, 'variation': 6, 'index': 1},                                           # unset
    {'topic': 's/bi0', 'group': 1, 'variation': 2, 'index': 0, 'initial_value': True, 'event_class': 2},
    {'topic': 's/bo0', 'group': 10, 'variation': 2, 'index': 0, 'initial_value': False},                   # remote-writable by default
    {'topic': 's/bo1', 'group': 10, 'variation': 2, 'index': 1, 'remote_writable': False},
    {'topic': 's/ao0', 'group': 40, 'variation': 1, 'index': 0, 'scaling': 10, 'initial_value': 100},
    {'topic': 's/ctr0', 'group': 20, 'variation': 1, 'index': 0, 'initial_value': 42},
]
MASTER_POINTS = [{k: v for k, v in p.items() if k in ('topic', 'group', 'variation', 'index', 'scaling')} for p in SERVED_POINTS]


class Pushes:
    def __init__(self):
        self.values = []

    async def __call__(self, values):
        self.values.append(values)


@pytest.fixture
async def served():
    pushes = Pushes()
    s = ServedOutstation('127.0.0.1', 0, outstation_address=1, master_address=2, push=pushes)
    s.configure_points(SERVED_POINTS)
    await s.start()
    s.pushes = pushes
    try:
        yield s
    finally:
        await s.close()


@pytest.fixture
async def master(served):
    host, port = served.runner.local_address
    client = OutstationClient(host, port, master_address=2, outstation_address=1, response_timeout=3.0,
                              integrity_poll_interval=0)
    client.configure_points(MASTER_POINTS)
    try:
        yield client
    finally:
        await client.close()


def test_served_point_defaults_and_rejections():
    p = ServedPoint.from_dict({'topic': 't', 'group': 40, 'index': 3, 'scaling': 10})
    assert (p.event_class, p.remote_writable, p.initial_value) == (1, True, None)
    assert ServedPoint.from_dict({'topic': 't', 'group': 30, 'index': 3}).remote_writable is False
    assert ServedPoint.from_dict({'topic': 't', 'group': 30, 'index': 3, 'remote_writable': True}).remote_writable is True
    assert p.raw_typed(250) == 25.0 and ServedPoint.from_dict({'topic': 't', 'group': 20, 'index': 1}).raw_typed('7') == 7
    with pytest.raises(ValueError, match='event class'):
        ServedPoint.from_dict({'topic': 't', 'group': 30, 'index': 3, 'event_class': 5})
    with pytest.raises(ValueError, match='cannot be served'):
        ServedPoint.from_dict({'topic': 't', 'group': 3, 'index': 0})          # double-bit inputs have no database


async def test_served_values_are_seeded_read_and_written(served):
    results, errors = await served.read(['s/ai0', 's/ai1', 's/bi0', 's/ctr0', 's/nope'])
    assert errors == {'s/nope': 'unregistered topic'}
    assert results['s/ai0']['value'] == pytest.approx(240.1) and results['s/ai0']['online'] is True
    assert results['s/ai1']['online'] is False and results['s/ai1']['value'] == 0       # RESTART quality: never set
    assert results['s/bi0']['value'] is True and results['s/ctr0']['value'] == 42
    results, errors = await served.write([{'topic': 's/ai1', 'value': 7.5}, {'topic': 's/bi0', 'value': 'off'},
                                          {'topic': 's/ai0', 'value': 'warm'}, {'topic': 's/zz', 'value': 1}])
    assert results == {'s/ai1': {'status': 'SUCCESS', 'value': 7.5}, 's/bi0': {'status': 'SUCCESS', 'value': 'off'}}
    assert set(errors) == {'s/ai0', 's/zz'} and 'bad value' in errors['s/ai0']
    results, _ = await served.read(['s/ai1', 's/bi0'])
    assert results['s/ai1']['value'] == 7.5 and results['s/ai1']['online'] is True and results['s/bi0']['value'] is False
    assert served.pushes.values == []                                                   # platform writes are not pushed back


async def test_push_all_carries_the_set_values_only(served):
    values = await served.push_all()
    assert values == {'s/ai0': pytest.approx(240.1), 's/bi0': True, 's/bo0': False, 's/ao0': 100.0, 's/ctr0': 42}
    assert served.pushes.values == [values]


async def test_registration_values_seed_new_points_ahead_of_starting_values(served):
    # Points already held keep their values; new points take the caller's value over the row's initial_value.
    assert served.configure_points(SERVED_POINTS + [{'topic': 's/new', 'group': 30, 'index': 9, 'initial_value': 1}],
                                   {'s/ai0': 1.0, 's/new': 5, 's/zz': 3}) == len(SERVED_POINTS) + 1
    assert served.seeded == 1
    results, _ = await served.read(['s/ai0', 's/new'])
    assert results['s/ai0']['value'] == pytest.approx(240.1) and results['s/new']['value'] == 5
    fresh = ServedOutstation('127.0.0.1', 0)
    assert fresh.configure_points(SERVED_POINTS, {'s/ai0': 1.0, 's/ai1': 'x', 's/bi0': 'off'}) == len(SERVED_POINTS)
    assert fresh.seeded == 2                                                             # 'x' is not a number: ignored
    results, _ = await fresh.read(['s/ai0', 's/ai1', 's/bi0', 's/ao0'])
    assert results['s/ai0']['value'] == pytest.approx(1.0) and results['s/bi0']['value'] is False
    assert results['s/ai1']['online'] is False and results['s/ao0']['value'] == 100.0    # no seed: starting value
    assert served.pushes.values == []


async def test_reconfiguration_keeps_values_of_points_that_stay(served):
    await served.write([{'topic': 's/ai1', 'value': 3}])
    count = served.configure_points(SERVED_POINTS[:2] + [{'topic': 's/new', 'group': 30, 'index': 9, 'initial_value': 1}])
    assert count == 3 and set(served.database.analog_inputs) == {0, 1, 9} and not served.database.binary_inputs
    results, _ = await served.read(['s/ai1', 's/new'])
    assert results['s/ai1']['value'] == 3 and results['s/new']['value'] == 1
    with pytest.raises(ValueError, match='both name'):
        served.configure_points([{'topic': 'a', 'group': 30, 'index': 1}, {'topic': 'b', 'group': 30, 'index': 1}])


async def test_a_master_reads_what_the_platform_wrote_and_its_controls_are_pushed(served, master):
    results, errors = await master.read(['s/ai0', 's/bi0', 's/ao0', 's/ctr0', 's/bo0'], mode='integrity')
    assert errors == {}
    assert results['s/ai0']['value'] == pytest.approx(240.1) and results['s/bi0']['value'] is True
    assert results['s/ao0']['value'] == 100.0 and results['s/ctr0']['value'] == 42 and results['s/bo0']['value'] is False

    await served.write([{'topic': 's/ai0', 'value': 230.0}])
    results, _ = await master.read(['s/ai0'], mode='class', classes=[0])
    assert results['s/ai0']['value'] == pytest.approx(230.0)

    results, errors = await master.write([{'topic': 's/bo0', 'value': True}, {'topic': 's/ao0', 'value': 250}])
    assert errors == {} and set(results) == {'s/bo0', 's/ao0'}
    await asyncio.sleep(0.05)
    pushed = {}
    for values in served.pushes.values:
        pushed.update(values)
    assert pushed == {'s/bo0': True, 's/ao0': 250.0}
    results, _ = await served.read(['s/bo0', 's/ao0'])
    assert results['s/bo0']['value'] is True and results['s/ao0']['value'] == 250.0

    results, errors = await master.write([{'topic': 's/bo1', 'value': True}], control_mode='sbo')
    assert results == {} and errors == {'s/bo1': 'status BLOCKED'}                 # not remote-writable
    results, errors = await master.write([{'topic': 's/bo1', 'value': True}])
    assert errors == {'s/bo1': 'status BLOCKED'}


async def test_proxy_registers_serves_and_closes_an_outstation():
    proxy = make_proxy()
    pushes = []

    async def fake_push(client, values):
        pushes.append((client.remote_id, values))
    proxy._push = fake_push
    remote = uuid4()
    reply = await call(proxy, proxy.register_outstation_endpoint,
                       {'role': 'outstation', 'host': '127.0.0.1', 'port': 0, 'outstation_address': 4, 'master_address': 3,
                        'points': SERVED_POINTS, 'values': {'s/ai1': 11, 's/ai0': 0}}, remote=remote)
    key = 'outstation:127.0.0.1:0:4'
    assert reply == {'result': {'server': key, 'points': len(SERVED_POINTS), 'role': 'outstation', 'seeded': 1}, 'error': {}}
    served = proxy.servers[key]
    assert proxy.remotes[remote] is served and served.runner.is_running
    await asyncio.sleep(0.05)
    assert pushes and pushes[0][0] == remote and pushes[0][1]['s/ctr0'] == 42       # the table, pushed on registration
    assert pushes[0][1]['s/ai1'] == 11 and pushes[0][1]['s/ai0'] == pytest.approx(240.1)   # seeded, and kept

    reply = await call(proxy, proxy.read_points_endpoint, {'mode': 'class', 'topics': ['s/ai0']}, remote=remote)
    assert reply['result']['s/ai0']['value'] == pytest.approx(240.1)
    reply = await call(proxy, proxy.write_points_endpoint, {'operations': [{'topic': 's/ai0', 'value': 1.5}]}, remote=remote)
    assert reply == {'result': {'s/ai0': {'status': 'SUCCESS', 'value': 1.5}}, 'error': {}}

    # Re-registering with the same listener keeps the served outstation; a changed table replaces the points.
    reply = await call(proxy, proxy.register_outstation_endpoint,
                       {'role': 'server', 'host': '127.0.0.1', 'port': 0, 'outstation_address': 4, 'points': SERVED_POINTS[:1]},
                       remote=remote)
    assert reply['result']['points'] == 1 and proxy.servers[key] is served

    reply = await call(proxy, proxy.register_outstation_endpoint, {'role': 'sideways', 'points': []}, remote=remote)
    assert 'Unknown role' in reply['error']['outstation']
    reply = await call(proxy, proxy.register_outstation_endpoint, {'role': 'outstation', 'port': 0, 'points': [
        {'topic': 'x', 'group': 3, 'index': 0}]}, remote=uuid4())
    assert 'cannot be served' in reply['error']['outstation'] and len(proxy.servers) == 1

    reply = await call(proxy, proxy.close_outstation_endpoint, {}, remote=remote)
    assert reply['result'] == {'closed': True} and proxy.servers == {} and proxy.remotes == {}
    assert not served.runner.is_running


async def test_registering_a_second_outstation_on_a_busy_port_is_an_error():
    proxy = make_proxy()
    proxy._push = lambda client, values: asyncio.sleep(0)
    first = await call(proxy, proxy.register_outstation_endpoint,
                       {'role': 'outstation', 'host': '127.0.0.1', 'port': 0, 'outstation_address': 1, 'points': []})
    port = proxy.servers[first['result']['server']].runner.local_address[1]
    try:
        reply = await call(proxy, proxy.register_outstation_endpoint,
                           {'role': 'outstation', 'host': '127.0.0.1', 'port': port, 'outstation_address': 2, 'points': []},
                           remote=uuid4())
        assert reply['result'] == {} and reply['error']['outstation'] and len(proxy.servers) == 1
    finally:
        for served in list(proxy.servers.values()):
            await served.close()
