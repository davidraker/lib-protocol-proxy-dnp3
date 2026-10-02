"""Every endpoint through the real IPC base class, with the proxy constructed but never registered."""
import json
from unittest import mock
from uuid import uuid4

import pytest

import protocol_proxy.protocol.dnp3 as package
from protocol_proxy.proxy.launch import resolve_launcher
from protocol_proxy.protocol.dnp3.dnp3_proxy import Dnp3Proxy
from protocol_proxy.protocol.dnp3.json import serialize
from tests.conftest import MASTER, OUTSTATION, POINTS


def make_proxy() -> Dnp3Proxy:
    return Dnp3Proxy(proxy_id=uuid4(), token=uuid4(), manager_address='127.0.0.1', manager_port=1, manager_id=uuid4(),
                     manager_token=uuid4(), registration_retry_delay=0, registration_timeout=0.05)


async def call(proxy, endpoint, message: dict) -> dict:
    """Invoke an endpoint the way the IPC layer does, past the peer-authentication wrapper."""
    reply = await endpoint.__wrapped__(proxy, None, json.dumps(message).encode('utf8'))
    return json.loads(reply)


def identity(served) -> dict:
    return {'host': served.host, 'port': served.port, 'master_address': MASTER, 'outstation_address': OUTSTATION}


def test_serialize_shapes():
    body = json.loads(serialize({'a': {'value': 1.5, 'timestamp': None}}, {'b': 'bad'}))
    assert body == {'result': {'a': {'value': 1.5, 'timestamp': None}}, 'error': {'b': 'bad'}}
    assert json.loads(serialize({}))['error'] == {}


async def test_callbacks_registered_and_launch_shape():
    proxy = make_proxy()
    assert {'REGISTER_OUTSTATION', 'READ_POINTS', 'WRITE_POINTS', 'CLOSE_OUTSTATION'} <= set(proxy.callbacks)
    assert all(proxy.callbacks[n].timeout == 120.0 for n in ('READ_POINTS', 'WRITE_POINTS'))
    assert Dnp3Proxy.LAUNCHER is None and Dnp3Proxy.PATCH_GEVENT is False and package.PROXY_CLASS is Dnp3Proxy
    assert Dnp3Proxy.get_unique_remote_id(['dnp3', 'site']) == ('dnp3', 'site')
    assert resolve_launcher('protocol_proxy.protocol.dnp3.dnp3_proxy:Dnp3Proxy') is not None


async def test_register_read_write_close(served):
    proxy = make_proxy()
    reply = await call(proxy, proxy.register_outstation_endpoint, {**identity(served), 'response_timeout': 3.0,
                                                                   'integrity_poll_interval': 0, 'points': POINTS})
    key = f'{served.host}:{served.port}:{OUTSTATION}:{MASTER}'
    assert reply == {'result': {'client': key, 'points': len(POINTS)}, 'error': {}}
    assert key in proxy.clients and proxy.clients[key]._listener is None     # unsolicited off: no listener

    reply = await call(proxy, proxy.read_points_endpoint, {**identity(served), 'mode': 'integrity', 'topics': ['d/ai0', 'd/ai1']})
    assert reply['error'] == {}
    assert reply['result']['d/ai0']['value'] == 2401.0 and reply['result']['d/ai1']['value'] == pytest.approx(-15.5)
    assert reply['result']['d/ai0']['online'] is True

    reply = await call(proxy, proxy.write_points_endpoint, {**identity(served), 'control_mode': 'sbo',
                                                            'operations': [{'topic': 'd/ao1', 'value': 33.5}]})
    assert reply == {'result': {'d/ao1': {'status': 'SUCCESS', 'value': 33.5}}, 'error': {}}

    # Re-registering replaces the point table and keeps the client.
    client = proxy.clients[key]
    reply = await call(proxy, proxy.register_outstation_endpoint, {**identity(served), 'points': POINTS[:2]})
    assert reply['result']['points'] == 2 and proxy.clients[key] is client

    reply = await call(proxy, proxy.close_outstation_endpoint, identity(served))
    assert reply['result'] == {'closed': True} and proxy.clients == {}
    reply = await call(proxy, proxy.close_outstation_endpoint, identity(served))
    assert reply['result'] == {'closed': False}


async def test_unknown_outstation_and_bad_registration(served):
    proxy = make_proxy()
    reply = await call(proxy, proxy.read_points_endpoint, {**identity(served), 'topics': ['d/ai0']})
    assert reply['result'] == {} and reply['error'] == {'outstation': 'Outstation not registered.'}
    reply = await call(proxy, proxy.register_outstation_endpoint, {'port': 20000})
    assert 'host' in reply['error']['outstation']
    reply = await call(proxy, proxy.register_outstation_endpoint, {**identity(served), 'points': [{'topic': 'x', 'group': 12, 'index': 0}]})
    assert 'not a point the proxy reads' in reply['error']['outstation'] and proxy.clients == {}


async def test_push_sends_receive_unsolicited_to_the_manager():
    proxy = make_proxy()
    proxy.send = mock.AsyncMock(return_value=True)
    await proxy._push({'d/ai0': 5.5})
    peer, message = proxy.send.await_args.args
    assert peer is proxy.peers[proxy.manager]
    assert message.method_name == 'RECEIVE_UNSOLICITED' and message.response_expected is False
    assert json.loads(message.payload) == {'result': {'d/ai0': 5.5}, 'error': {}}
