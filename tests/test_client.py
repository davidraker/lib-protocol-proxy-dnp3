"""OutstationClient against a live dnp3py Outstation over the loopback interface."""
import asyncio
from pathlib import Path
from importlib import resources

import pytest

from dnp3.core.enums import ControlCode
from dnp3.objects.layout import PointKind

from protocol_proxy.protocol.dnp3.client import OutstationClient, PointSpec, _spans
from tests.conftest import MASTER, OUTSTATION, POINTS, make_client


def values(results: dict) -> dict:
    return {topic: entry['value'] for topic, entry in results.items()}


class TestPointSpecs:
    def test_defaults_and_scaling(self):
        spec = PointSpec.from_dict({'topic': 't', 'group': 30, 'index': 4, 'variation': None, 'scaling': ''})
        assert (spec.variation, spec.kind, spec.scaling, spec.control_code) == (6, PointKind.ANALOG_INPUT, 1.0, 'latch')
        scaled = PointSpec.from_dict({'topic': 't', 'group': 40, 'index': 1, 'variation': 2, 'scaling': '0.5'})
        assert scaled.raw(10) == 20 and scaled.kind is PointKind.ANALOG_OUTPUT

    def test_unreadable_group_is_refused(self):
        with pytest.raises(ValueError, match='not a point the proxy reads'):
            PointSpec.from_dict({'topic': 't', 'group': 12, 'index': 0, 'variation': 1})
        with pytest.raises(ValueError):
            PointSpec.from_dict({'topic': 't', 'group': 30, 'index': 0, 'variation': 9})

    def test_spans(self):
        assert _spans([1, 2, 3, 7, 9, 10]) == [(1, 3), (7, 7), (9, 10)] and _spans([]) == []


class TestReads:
    async def test_integrity_read_reports_every_point_scaled(self, served):
        client = make_client(served)
        results, errors = await client.read([p['topic'] for p in POINTS], mode='integrity')
        assert errors == {}
        got = values(results)
        assert got['d/bi0'] is True and got['d/bi1'] is False and got['d/bo0'] is False
        assert got['d/ai0'] == 2401.0 and got['d/ai1'] == pytest.approx(-15.5) and got['d/ai2'] == 7.0
        assert got['d/ao0'] == 100.0 and got['d/ao1'] == 20.0 and got['d/ctr0'] == 42
        assert all(entry['online'] for entry in results.values())
        await client.close()

    async def test_class_zero_read_refreshes_static_values(self, served):
        client = make_client(served, integrity_poll_interval=0)
        await client.read(['d/ai0'])                       # first read: the startup integrity poll
        served.database.update_analog_input(0, 99)
        results, _ = await client.read(['d/ai0'], mode='class', classes=[0])
        assert values(results) == {'d/ai0': 99.0}
        await client.close()

    async def test_points_read_issues_one_range_request_per_span(self, served):
        """dnp3py's outstation answers a range read with every point of the group, so the requests themselves are
        what this checks: one READ per contiguous span of each (group, variation), nothing else."""
        client = make_client(served, points=POINTS + [{'topic': 'd/bi1b', 'group': 1, 'variation': 1, 'index': 1}])
        await client.read(['d/ai0'])                                        # startup integrity poll
        sent = []
        original = client.runner.request

        async def spy(request):
            sent.append([(b.header.group, b.header.variation, b.data) for b in request.objects])
            return await original(request)

        client.runner.request = spy
        served.database.update_analog_input(0, 5)
        results, errors = await client.read(['d/ai2', 'd/ai0', 'd/bi1b'], mode='points')
        assert errors == {} and values(results)['d/ai0'] == 5.0 and values(results)['d/bi1b'] is False
        assert sent == [[(30, 6, bytes([0, 0]))], [(30, 6, bytes([2, 2]))], [(1, 1, bytes([1, 1]))]]
        await client.close()

    async def test_integrity_interval_upgrades_a_read_when_due(self, served):
        client = make_client(served, integrity_poll_interval=0.05)
        await client.read(['d/ai0'])
        first = client._last_integrity
        await client.read(['d/ai0'], mode='class')
        assert client._last_integrity == first                  # not due yet
        await asyncio.sleep(0.06)
        await client.read(['d/ai0'], mode='class')
        assert client._last_integrity > first                   # upgraded to an integrity poll
        await client.close()

    async def test_unregistered_and_unreported_topics(self, served):
        client = make_client(served, points=POINTS + [{'topic': 'd/ai9', 'group': 30, 'variation': 6, 'index': 9}])
        results, errors = await client.read(['d/ai0', 'd/ai9', 'nope'])
        assert set(results) == {'d/ai0'}
        assert errors == {'nope': 'unregistered topic', 'd/ai9': 'not reported by outstation'}
        await client.close()

    async def test_bad_mode(self, served):
        client = make_client(served)
        results, errors = await client.read(['d/ai0'], mode='magic')
        assert results == {} and 'Unknown read mode' in errors['link']


class TestWrites:
    async def test_direct_operate_then_read_back(self, served):
        client = make_client(served)
        results, errors = await client.write([{'topic': 'd/ao1', 'value': 12.5}, {'topic': 'd/bo0', 'value': True}])
        assert errors == {}
        assert results == {'d/ao1': {'status': 'SUCCESS', 'value': 12.5}, 'd/bo0': {'status': 'SUCCESS', 'value': True}}
        assert ('binary', 0, ControlCode.LATCH_ON) in served.handler.operated
        assert ('analog', 1, pytest.approx(12.5)) in [(k, i, v) for k, i, v in served.handler.operated]
        read, _ = await client.read(['d/ao1', 'd/bo0'])
        assert values(read) == {'d/ao1': 12.0, 'd/bo0': True}      # the outstation serves AO status as integers
        await client.close()

    async def test_scaling_divides_on_write(self, served):
        client = make_client(served)
        results, errors = await client.write([{'topic': 'd/ao0', 'value': 120}])
        assert errors == {} and results['d/ao0']['value'] == 120
        assert ('analog', 0, 12.0) in served.handler.operated            # raw value reaches the outstation
        read, _ = await client.read(['d/ao0'])
        assert values(read) == {'d/ao0': 120.0}
        await client.close()

    async def test_pulse_control_code(self, served):
        client = make_client(served)
        _, errors = await client.write([{'topic': 'd/bo1', 'value': 'on'}, {'topic': 'd/bo0', 'value': 'off'}])
        assert errors == {}
        assert ('binary', 1, ControlCode.PULSE_ON) in served.handler.operated
        assert ('binary', 0, ControlCode.LATCH_OFF) in served.handler.operated
        await client.close()

    async def test_select_before_operate(self, served):
        client = make_client(served)
        results, errors = await client.write([{'topic': 'd/ao1', 'value': 50}], control_mode='sbo')
        assert errors == {} and results['d/ao1']['status'] == 'SUCCESS'
        assert ('analog', 1, 50.0) in served.handler.operated
        operated_before = list(served.handler.operated)
        results, errors = await client.write([{'topic': 'd/ao1', 'value': 5000}], control_mode='sbo')
        assert results == {} and errors == {'d/ao1': 'status OUT_OF_RANGE'}
        assert served.handler.operated == operated_before                 # refused SELECT: nothing operated
        await client.close()

    async def test_refused_direct_operate_reports_status(self, served):
        client = make_client(served)
        results, errors = await client.write([{'topic': 'd/ao1', 'value': 5000}, {'topic': 'd/bo0', 'value': True}])
        assert set(results) == {'d/bo0'} and errors == {'d/ao1': 'status OUT_OF_RANGE'}
        await client.close()

    async def test_local_rejections(self, served):
        client = make_client(served)
        results, errors = await client.write([{'topic': 'd/ai0', 'value': 1}, {'topic': 'd/bo0', 'value': 'maybe'},
                                              {'topic': 'nope', 'value': 1}], control_mode='direct')
        assert results == {}
        assert errors['d/ai0'] == 'analog_input is not an output' and errors['nope'] == 'unregistered topic'
        assert errors['d/bo0'].startswith('bad value')
        _, errors = await client.write([{'topic': 'd/bo0', 'value': True}], control_mode='maybe')
        assert 'Unknown control mode' in errors['link']


class TestLink:
    async def test_reopens_after_the_link_is_closed(self, served):
        client = make_client(served)
        await client.read(['d/ai0'])
        await client.runner.close()
        results, errors = await client.read(['d/ai0'])
        assert errors == {} and values(results) == {'d/ai0': 2401.0}
        await client.close()

    async def test_failed_link_is_reported_and_dropped(self, served):
        client = make_client(served)
        await client.read(['d/ai0'])
        from tests.conftest import stop
        await stop(served)
        results, errors = await client.read(['d/ai0'])
        assert results == {} and 'link' in errors and not client.runner.is_open
        await client.close()

    async def test_unreachable_outstation(self):
        client = OutstationClient('127.0.0.1', 1, master_address=MASTER, outstation_address=OUTSTATION, response_timeout=1.0)
        client.configure_points(POINTS[:1])
        results, errors = await client.read(['d/bi0'])
        assert results == {} and errors['link'].startswith('LinkError')


class TestMesa:
    async def test_mesa_point_set_reads(self):
        """The MESA IEEE 1815.2 simulator shipped with dnp3py serves the fake_dnp3 case's kind of point set."""
        from dnp3.mesa import create_mesa_outstation
        profile = Path(str(resources.files('dnp3.mesa').joinpath('data/profiles/mandatory_1547.json')))
        mesa = create_mesa_outstation(profile, host='127.0.0.1', port=0, address=OUTSTATION, master_address=MASTER)
        task = asyncio.create_task(mesa.run())
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 5
        while not (mesa.is_running and mesa.local_address):
            assert loop.time() < deadline, 'MESA outstation did not bind'
            await asyncio.sleep(0.02)
        host, port = mesa.local_address
        client = OutstationClient(host, port, master_address=MASTER, outstation_address=OUTSTATION, response_timeout=5.0)
        client.configure_points([{'topic': 'AI_2', 'group': 30, 'variation': 6, 'index': 2},
                                 {'topic': 'BI_0', 'group': 1, 'variation': 2, 'index': 0},
                                 {'topic': 'BO_3', 'group': 10, 'variation': 2, 'index': 3},
                                 {'topic': 'CTR_5000', 'group': 20, 'variation': 1, 'index': 5000}])
        try:
            results, errors = await client.read(['AI_2', 'BI_0', 'BO_3', 'CTR_5000'], mode='integrity')
            assert errors == {} and set(results) == {'AI_2', 'BI_0', 'BO_3', 'CTR_5000'}
            written, werrors = await client.write([{'topic': 'BO_3', 'value': True}])
            assert werrors == {} and written['BO_3']['status'] == 'SUCCESS'
        finally:
            await client.close()
            await mesa.stop()
            task.cancel()
            with pytest.raises((asyncio.CancelledError, TimeoutError)):
                await asyncio.wait_for(task, timeout=5)
