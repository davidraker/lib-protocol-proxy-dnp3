"""Curves and schedules stored behind the edit buffers of a served outstation (multiplex.py), exercised by the proxy's
own master client: the selector chooses the stored instance the buffer shows, store rows hold the data, the rules of
IEEE 1815.2-2025 6.1.3 and 6.1.4.3 apply."""
import asyncio

import pytest

from protocol_proxy.protocol.dnp3.client import OutstationClient
from protocol_proxy.protocol.dnp3.multiplex import EDITING, ENABLED, INVALID, NOT_IN_USE, VALIDATED
from protocol_proxy.protocol.dnp3.server import ServedOutstation
from tests.test_server import Pushes


def curve_rows(slots=2):
    rows = [{'topic': 's/curve_Selector', 'group': 40, 'index': 244, 'mux_area': 'curve', 'mux_slot': 'selector', 'readback': {'group': 30, 'index': 328}}]
    for slot in range(1, slots + 1):
        for name, ao, ai in (('ModTyp', 245, 329), ('NumPts', 246, 330), ('X1', 249, 333), ('Y1', 250, 334), ('X2', 251, 335), ('Y2', 252, 336)):
            # pairs are scaled integers on the wire (the standard's multipliers); the store row carries the scaling
            rows.append({'topic': f's/curve{slot}_{name}', 'group': 40, 'index': ao, 'mux_area': 'curve', 'mux_slot': slot, 'readback': {'group': 30, 'index': ai},
                         'scaling': 0.01 if name[0] in 'XY' else 1})
    return rows


FUNCTION_ROWS = [
    {'topic': 's/AO_217', 'group': 40, 'index': 217, 'readback': {'group': 30, 'index': 297}, 'initial_value': 0},   # Volt-Var curve index
    {'topic': 's/BO_29', 'group': 10, 'index': 29, 'initial_value': False},                                       # Volt-Var enable
    {'topic': 's/BI_107', 'group': 1, 'index': 107, 'initial_value': False},                                      # selected curve is referenced
]
CURVE_AREAS = {'curve': {'references': [{'point': {'group': 40, 'index': 217}, 'enable': {'group': 10, 'index': 29}}],
                         'referenced_flag': {'group': 1, 'index': 107}}}

# the master's view: the wire buffer and the function points, by their own topics
MASTER_POINTS = [{'topic': 'm/sel', 'group': 40, 'index': 244}, {'topic': 'm/sel_rb', 'group': 30, 'index': 328},
                 {'topic': 'm/type', 'group': 40, 'index': 245}, {'topic': 'm/numpts', 'group': 40, 'index': 246},
                 {'topic': 'm/x1', 'group': 40, 'index': 249, 'scaling': 0.01}, {'topic': 'm/y1', 'group': 40, 'index': 250, 'scaling': 0.01}, {'topic': 'm/x2', 'group': 40, 'index': 251, 'scaling': 0.01},
                 {'topic': 'm/x1_rb', 'group': 30, 'index': 333, 'scaling': 0.01}, {'topic': 'm/y1_rb', 'group': 30, 'index': 334, 'scaling': 0.01}, {'topic': 'm/type_rb', 'group': 30, 'index': 329},
                 {'topic': 'm/crvidx', 'group': 40, 'index': 217}, {'topic': 'm/crvidx_rb', 'group': 30, 'index': 297},
                 {'topic': 'm/ena', 'group': 10, 'index': 29}, {'topic': 'm/referenced', 'group': 1, 'index': 107}]


@pytest.fixture
async def served():
    pushes = Pushes()
    s = ServedOutstation('127.0.0.1', 0, outstation_address=1, master_address=2, push=pushes)
    s.configure_points(curve_rows() + FUNCTION_ROWS, {}, CURVE_AREAS)
    await s.start()
    s.pushes = pushes
    try:
        yield s
    finally:
        await s.close()


@pytest.fixture
async def master(served):
    host, port = served.runner.local_address
    client = OutstationClient(host, port, master_address=2, outstation_address=1, response_timeout=3.0, integrity_poll_interval=0)
    client.configure_points(MASTER_POINTS)
    try:
        yield client
    finally:
        await client.close()


async def read(master, *topics):
    results, errors = await master.read(list(topics), mode='integrity')
    assert errors == {}, errors
    return {t: (results[t]['value'], results[t]['online']) for t in topics}


def pushed(served):
    out = {}
    for values in served.pushes.values:
        out.update(values)
    return out


def test_configuration_builds_the_areas_and_the_wire_buffer(served):
    area = served.mux.areas['curve']
    assert area.slots == 2 and area.selected == 0 and area.selector == served.mux.by_selector[area.selector].selector
    assert len(area.buffer) == 6 and area.buffer[(area.buffer and list(area.buffer)[0])] is not None
    assert 's/curve2_X1' in served.mux.by_topic and 's/curve2_X1' not in served.points, 'a store row is not a wire point of its own'
    assert 249 in served.database.analog_outputs and 333 in served.database.analog_inputs, 'the buffer and its read-backs exist on the wire'
    assert int(served.database.analog_inputs[333].quality) == 2, 'nothing selected: RESTART'
    described = served.describe()
    assert described['points'] == 3 + 1 + 12 and described['areas'][0]['slots'] == 2


async def test_selector_chooses_the_instance_the_buffer_shows(served, master):
    results, errors = await master.write([{'topic': 'm/x1', 'value': 0.9}])
    assert errors == {'m/x1': 'status OUT_OF_RANGE'}, 'nothing selected: refused, not stored'
    results, errors = await master.write([{'topic': 'm/sel', 'value': 3}])
    assert errors == {'m/sel': 'status OUT_OF_RANGE'}, 'two curves are stored; the third does not exist (6.1.3 a)'
    results, errors = await master.write([{'topic': 'm/sel', 'value': 1}])
    assert errors == {}
    values = await read(master, 'm/sel_rb', 'm/x1_rb')
    assert values['m/sel_rb'] == (1.0, True) and values['m/x1_rb'][1] is False, 'curve 1 selected, never written: RESTART'
    results, errors = await master.write([{'topic': 'm/type', 'value': 2}, {'topic': 'm/numpts', 'value': 1}, {'topic': 'm/x1', 'value': 0.92}, {'topic': 'm/y1', 'value': 0.44}])
    assert errors == {}
    await asyncio.sleep(0.05)
    assert pushed(served) == {'s/curve_Selector': 1.0, 's/curve1_ModTyp': 2.0, 's/curve1_NumPts': 1.0, 's/curve1_X1': 0.92, 's/curve1_Y1': 0.44}, 'pushes name the store rows, not the buffer'
    values = await read(master, 'm/x1_rb', 'm/y1_rb', 'm/type_rb')
    assert values['m/x1_rb'] == (pytest.approx(0.92), True) and values['m/type_rb'] == (2.0, True)
    # pairs beyond the number of points are stored as written (6.1.3 c: unused, not invalid)
    results, errors = await master.write([{'topic': 'm/x2', 'value': 1.08}])
    assert errors == {}
    # curve 2: its own storage
    await master.write([{'topic': 'm/sel', 'value': 2}])
    values = await read(master, 'm/x1_rb', 'm/sel_rb')
    assert values['m/x1_rb'][1] is False and values['m/sel_rb'] == (2.0, True)
    await master.write([{'topic': 'm/x1', 'value': 0.8}])
    await master.write([{'topic': 'm/sel', 'value': 1}])
    values = await read(master, 'm/x1_rb')
    assert values['m/x1_rb'] == (pytest.approx(0.92), True), 'curve 1 kept its contents while curve 2 was edited'
    results, _ = await served.read(['s/curve1_X1', 's/curve2_X1', 's/curve2_Y1', 's/curve1_X2'])
    assert results['s/curve1_X1']['value'] == pytest.approx(0.92) and results['s/curve2_X1']['value'] == pytest.approx(0.8)
    assert results['s/curve2_Y1']['online'] is False and results['s/curve1_X2']['value'] == pytest.approx(1.08)


async def test_a_referenced_curve_is_locked_while_its_function_is_enabled(served, master):
    await master.write([{'topic': 'm/sel', 'value': 1}, {'topic': 'm/x1', 'value': 0.9}])
    assert (await read(master, 'm/referenced'))['m/referenced'] == (False, True)
    await master.write([{'topic': 'm/crvidx', 'value': 1}])              # Volt-Var references curve 1
    values = await read(master, 'm/referenced', 'm/crvidx_rb')
    assert values['m/referenced'] == (True, True) and values['m/crvidx_rb'] == (1.0, True), 'the flag follows the reference; the read-back rule shows the index back'
    results, errors = await master.write([{'topic': 'm/x1', 'value': 0.5}])
    assert errors == {}, 'referenced but the function is not enabled: editable'
    await master.write([{'topic': 'm/ena', 'value': True}])
    results, errors = await master.write([{'topic': 'm/x1', 'value': 0.1}])
    assert errors == {'m/x1': 'status AUTOMATION_INHIBIT'}
    assert (await read(master, 'm/x1_rb'))['m/x1_rb'] == (pytest.approx(0.5), True), 'the locked curve is unchanged'
    ok, message = served.mux.write_from_platform('s/curve1_X1', 0.2)
    assert not ok and 'referenced by an enabled function' in message, 'the lock holds for the platform too'
    # the spare curve is editable, and repointing the function takes effect at once
    await master.write([{'topic': 'm/sel', 'value': 2}, {'topic': 'm/x1', 'value': 0.7}])
    assert (await read(master, 'm/referenced'))['m/referenced'] == (False, True)
    await master.write([{'topic': 'm/crvidx', 'value': 2}])
    assert (await read(master, 'm/referenced'))['m/referenced'] == (True, True)
    results, errors = await master.write([{'topic': 'm/crvidx', 'value': 0}])
    assert errors == {} and (await read(master, 'm/referenced'))['m/referenced'] == (False, True), 'curve index 0: no curve'
    assert served.describe()['areas'][0]['instances'][2]['locked'] is False


async def test_the_platform_writes_a_store_row_without_touching_the_selection(served, master):
    await master.write([{'topic': 'm/sel', 'value': 1}, {'topic': 'm/x1', 'value': 0.9}])
    results, errors = await served.write([{'topic': 's/curve2_X1', 'value': 0.55}, {'topic': 's/curve2_Y1', 'value': 'warm'}])
    assert results == {'s/curve2_X1': {'status': 'SUCCESS', 'value': 0.55}} and 'bad value' in errors['s/curve2_Y1']
    values = await read(master, 'm/sel_rb', 'm/x1_rb')
    assert values['m/sel_rb'] == (1.0, True) and values['m/x1_rb'] == (pytest.approx(0.9), True), 'the master still edits curve 1'
    await master.write([{'topic': 'm/sel', 'value': 2}])
    assert (await read(master, 'm/x1_rb'))['m/x1_rb'] == (pytest.approx(0.55), True)
    everything = await served.push_all()
    assert everything['s/curve2_X1'] == pytest.approx(0.55) and everything['s/curve1_X1'] == pytest.approx(0.9) and 's/curve2_Y1' not in everything


async def test_seeding_restores_the_stores_after_a_restart(served):
    fresh = ServedOutstation('127.0.0.1', 0)
    count = fresh.configure_points(curve_rows() + FUNCTION_ROWS, {'s/curve2_X1': 0.33, 's/curve_Selector': 2, 's/AO_217': 2}, CURVE_AREAS)
    assert count == 3 + 1 + 12 and fresh.seeded == 3, 'the selector, the curve index and one store row'
    results, _ = await fresh.read(['s/curve2_X1', 's/curve1_X1'])
    assert results['s/curve2_X1']['value'] == pytest.approx(0.33) and results['s/curve1_X1']['online'] is False
    assert fresh.mux.areas['curve'].selected == 2, 'the selection is restored with the selector'
    assert fresh.database.analog_inputs[333].value == 33 and int(fresh.database.analog_inputs[333].quality) == 1, 'and the buffer shows curve 2'


SCHEDULE_ROWS = [
    {'topic': 's/schedule_Selector', 'group': 40, 'index': 3000, 'mux_area': 'schedule', 'mux_slot': 'selector', 'readback': {'group': 30, 'index': 3000}},
    {'topic': 's/BO_3000', 'group': 10, 'index': 3000}, {'topic': 's/BO_3001', 'group': 10, 'index': 3001},
    {'topic': 's/BI_3000', 'group': 1, 'index': 3000}, {'topic': 's/BI_3001', 'group': 1, 'index': 3001},
    {'topic': 's/AI_3010', 'group': 30, 'index': 3010}, {'topic': 's/AI_3500', 'group': 30, 'index': 3500}, {'topic': 's/AI_3503', 'group': 30, 'index': 3503},
    {'topic': 's/AI_24', 'group': 30, 'index': 24}, {'topic': 's/AO_217', 'group': 40, 'index': 217},
]
for slot in (1, 2):
    for name, ao, ai in (('Schd', 3001, 3001), ('SchdPrio', 3002, 3002), ('NumEntr', 3009, 3011), ('Offset1', 3010, 3012), ('Type1', 3011, 3013), ('Index1', 3012, 3014), ('Val1', 3013, 3015),
                         ('Offset2', 3014, 3016), ('Type2', 3015, 3017), ('Index2', 3016, 3018), ('Val2', 3017, 3019)):
        SCHEDULE_ROWS.append({'topic': f's/schedule{slot}_{name}', 'group': 40, 'index': ao, 'mux_area': 'schedule', 'mux_slot': slot, 'readback': {'group': 30, 'index': ai}})
SCHEDULE_AREAS = {'schedule': {'schedule': {
    'submit': {'group': 10, 'index': 3000}, 'enable': {'group': 10, 'index': 3001}, 'validated': {'group': 1, 'index': 3000}, 'enabled': {'group': 1, 'index': 3001},
    'status': {'group': 30, 'index': 3010}, 'id': {'group': 40, 'index': 3001}, 'priority': {'group': 40, 'index': 3002}, 'num_entries': {'group': 40, 'index': 3009},
    'entries': [{'offset': {'group': 40, 'index': 3010}, 'type': {'group': 40, 'index': 3011}, 'index': {'group': 40, 'index': 3012}, 'value': {'group': 40, 'index': 3013}},
                {'offset': {'group': 40, 'index': 3014}, 'type': {'group': 40, 'index': 3015}, 'index': {'group': 40, 'index': 3016}, 'value': {'group': 40, 'index': 3017}}],
    'blocks': {1: {'status': {'group': 30, 'index': 3500}}, 2: {'status': {'group': 30, 'index': 3503}}},
    'count': {'group': 30, 'index': 24}, 'targets': [{'group': 40, 'index': 217, 'minimum': 0, 'maximum': 100}]}}}
SCHEDULE_MASTER = [{'topic': 'm/sel', 'group': 40, 'index': 3000}, {'topic': 'm/id', 'group': 40, 'index': 3001}, {'topic': 'm/n', 'group': 40, 'index': 3009},
                   {'topic': 'm/off1', 'group': 40, 'index': 3010}, {'topic': 'm/type1', 'group': 40, 'index': 3011}, {'topic': 'm/idx1', 'group': 40, 'index': 3012}, {'topic': 'm/val1', 'group': 40, 'index': 3013},
                   {'topic': 'm/off2', 'group': 40, 'index': 3014}, {'topic': 'm/type2', 'group': 40, 'index': 3015}, {'topic': 'm/idx2', 'group': 40, 'index': 3016}, {'topic': 'm/val2', 'group': 40, 'index': 3017},
                   {'topic': 'm/submit', 'group': 10, 'index': 3000}, {'topic': 'm/enable', 'group': 10, 'index': 3001},
                   {'topic': 'm/validated', 'group': 1, 'index': 3000}, {'topic': 'm/enabled', 'group': 1, 'index': 3001},
                   {'topic': 'm/status', 'group': 30, 'index': 3010}, {'topic': 'm/block1', 'group': 30, 'index': 3500}, {'topic': 'm/count', 'group': 30, 'index': 24}]


@pytest.fixture
async def scheduled():
    s = ServedOutstation('127.0.0.1', 0, outstation_address=1, master_address=2, push=Pushes())
    s.configure_points(SCHEDULE_ROWS, {}, SCHEDULE_AREAS)
    await s.start()
    host, port = s.runner.local_address
    client = OutstationClient(host, port, master_address=2, outstation_address=1, response_timeout=3.0, integrity_poll_interval=0)
    client.configure_points(SCHEDULE_MASTER)
    try:
        yield s, client
    finally:
        await client.close()
        await s.close()


async def test_schedules_run_the_state_machine_to_enabled(scheduled):
    served, master = scheduled
    schedule = served.mux.schedules['schedule']
    assert (await read(master, 'm/count'))['m/count'] == (2.0, True), 'Number of System Schedules announces the stored count'
    results, errors = await master.write([{'topic': 'm/submit', 'value': True}])
    assert errors == {'m/submit': 'status OUT_OF_RANGE'}, 'nothing selected'
    await master.write([{'topic': 'm/sel', 'value': 1}])
    assert schedule.state(1) == EDITING and (await read(master, 'm/status', 'm/block1'))['m/status'] == (float(EDITING), True)
    await master.write([{'topic': 'm/id', 'value': 5}, {'topic': 'm/n', 'value': 2},
                        {'topic': 'm/off1', 'value': 60}, {'topic': 'm/type1', 'value': 1}, {'topic': 'm/idx1', 'value': 217}, {'topic': 'm/val1', 'value': 50},
                        {'topic': 'm/off2', 'value': 30}, {'topic': 'm/type2', 'value': 1}, {'topic': 'm/idx2', 'value': 217}, {'topic': 'm/val2', 'value': 150}])
    await master.write([{'topic': 'm/submit', 'value': True}])
    assert schedule.state(1) == INVALID, 'decreasing offset and a value above the target\'s maximum'
    problems = ' '.join(schedule.validate(1))
    assert 'before entry 1' in problems and 'outside 0 to 100' in problems
    values = await read(master, 'm/validated', 'm/status', 'm/block1')
    assert values['m/validated'] == (False, True) and values['m/status'] == (float(INVALID), True) and values['m/block1'] == (float(INVALID), True)
    await master.write([{'topic': 'm/enable', 'value': True}])
    assert schedule.state(1) == INVALID and (await read(master, 'm/enabled'))['m/enabled'] == (False, True), 'enable without Validated bounces'
    await master.write([{'topic': 'm/off2', 'value': 120}, {'topic': 'm/val2', 'value': 80}])
    assert schedule.state(1) == EDITING, 'an edit returns the schedule to Editing'
    await master.write([{'topic': 'm/submit', 'value': True}])
    assert schedule.state(1) == VALIDATED and (await read(master, 'm/validated'))['m/validated'] == (True, True)
    await master.write([{'topic': 'm/enable', 'value': True}])
    assert schedule.state(1) == ENABLED
    values = await read(master, 'm/enabled', 'm/status', 'm/block1')
    assert values['m/enabled'] == (True, True) and values['m/block1'] == (float(ENABLED), True)
    assert served.describe()['areas'][0]['states'] == {1: ENABLED, 2: NOT_IN_USE}
    # schedule 2 is untouched by all of this
    await master.write([{'topic': 'm/sel', 'value': 2}])
    assert schedule.state(2) == EDITING and schedule.state(1) == ENABLED
    await master.write([{'topic': 'm/id', 'value': 0}])          # separate requests: a CROB block is processed before the AO block of one request
    await master.write([{'topic': 'm/submit', 'value': True}])
    assert schedule.state(2) == NOT_IN_USE, 'schedule ID 0 deletes'
    assert schedule.state(1) == ENABLED
