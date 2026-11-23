"""The test parties in protocol_proxy.protocol.dnp3.testing: the IEEE 1815.2 tool's profile helpers, and its
reference outstation (with Docker and IEEE1815_2_TEST_TOOL)."""
import socket

import pytest

from protocol_proxy.protocol.dnp3 import testing

PROFILE = {
    'AI': {'meters': [{'point_index': 1, 'value': 10, 'minimum': 0, 'maximum': 100},
                      {'point_index': 3, 'value': 2400, 'minimum': 0, 'maximum': 3000}],
           'name': 'not a list'},
    'AO': [{'point_index': 87, 'assoc_ai': 'AI_1'}],
    'BI': [{'point_index': 1}], 'BO': [{'point_index': 17}, {'point_index': 18}],
}


def test_get_and_set_point_across_sections():
    profile = {k: (v if not isinstance(v, dict) else {g: [dict(p) for p in pts] if isinstance(pts, list) else pts
                                                   for g, pts in v.items()}) for k, v in PROFILE.items()}
    assert testing.get_point(profile, 'AI', 3)['value'] == 2400
    testing.set_point(profile, 'AI', 3, value=2640)
    assert testing.get_point(profile, 'AI', 3)['value'] == 2640
    with pytest.raises(KeyError):
        testing.get_point(profile, 'AI', 99)


def test_with_stimulus_widens_bounds_and_targets_outputs():
    p = testing.with_stimulus(PROFILE, analog_values={3: 5000}, bounds={1: 42}, ao_targets={87: 55}, quiet=True)
    ai3 = testing.get_point(p, 'AI', 3)
    assert ai3['value'] == 5000 and ai3['minimum'] <= 5000 <= ai3['maximum']
    ai1 = testing.get_point(p, 'AI', 1)                       # bounded around 42, then the AO 87 target 55 widens it
    assert ai1['value'] == 55 and ai1['minimum'] <= 42 and ai1['maximum'] >= 55
    assert testing.get_point(PROFILE, 'AI', 3)['value'] == 2400   # the original is untouched


def test_trimmed_keeps_only_the_listed_points():
    p = testing.trimmed(PROFILE, ai=[3], bo=[17])
    assert [pt['point_index'] for pt in p['AI']['meters']] == [3] and p['AI']['name'] == 'not a list'
    assert p['AO'] == [] and [pt['point_index'] for pt in p['BO']] == [17]


def test_read_values_from_range_warnings():
    logs = ('x Profile conformance: AI 3 value 2640 out of range\n'
            'Profile conformance: AI 3 value 2641.5 out of range\nProfile conformance: AI 7 value -1 out of range\n')
    assert testing.read_values(logs) == {3: [2640.0, 2641.5], 7: [-1.0]}


@pytest.mark.skipif(not testing.available(), reason='needs Docker and the IEEE 1815.2 test tool (IEEE1815_2_TEST_TOOL)')
def test_the_reference_outstation_listens(tmp_path):
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
    outstation = testing.ReferenceOutstation(tmp_path, port, testing.load_profile(), name='dnp3-testing-party')
    try:
        outstation.start()
        socket.create_connection(('127.0.0.1', port), timeout=2).close()
    finally:
        outstation.stop()
