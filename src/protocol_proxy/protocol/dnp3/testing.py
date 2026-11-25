"""Test parties for the DNP3 proxy: the IEEE 1815.2 test tool's reference outstation and reference control station, as
DNP3 implementations independent of the proxy. Nothing here is imported by the proxy; the parties need Docker and the
tool (``IEEE1815_2_TEST_TOOL``), the profile helpers need neither.

Both are Rust binaries on the Step Function `dnp3` crate (not the dnp3py the proxy uses), already compiled into the
tool's cargo cache volume; they run in the tool's backend image on the host network. The outstation serves the DER
profile's points with the profile's ``value`` fields as its analog inputs; the control station polls an outstation and
writes every analog output whose associated analog input (``assoc_ai``) has a profile ``value`` (direct operate,
g41v1) and latches on every binary output whose associated binary input does not read true (g12v1). Verification of
what the control station received is through the profile's min/max: its MON_001 conformance test and its range
warnings tell whether the values it read lie within the bounds we set around the expected value."""
import copy
import json
import math
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

#: A checkout of https://github.com/pnnl/ieee-std-1815-2-test-tool, named by the IEEE1815_2_TEST_TOOL environment variable.
TOOL_REPO = Path(os.environ['IEEE1815_2_TEST_TOOL']).expanduser() if os.environ.get('IEEE1815_2_TEST_TOOL') else None
IMAGE = 'ieee-std-1815-2-test-tool-backend-dev:latest'
CACHE_VOLUME = 'ieee-std-1815-2-test-tool_cargo-cache'
ANSI = re.compile(r'\x1b\[[0-9;]*m')


def available() -> bool:
    if TOOL_REPO is None or not TOOL_REPO.is_dir():
        return False
    if shutil.which('docker') is None:    # no docker binary at all: not available, rather than FileNotFoundError
        return False
    ok = subprocess.run(['docker', 'image', 'inspect', IMAGE], capture_output=True).returncode == 0
    return ok and subprocess.run(['docker', 'volume', 'inspect', CACHE_VOLUME], capture_output=True).returncode == 0


def load_profile() -> dict:
    if TOOL_REPO is None:
        raise RuntimeError('Set IEEE1815_2_TEST_TOOL to a checkout of the IEEE 1815.2 test tool.')
    return json.loads((TOOL_REPO / 'data' / 'profiles' / 'full.json').read_text())


def _points(profile: dict, table: str):
    """Every point dict of a table across its sections (points, meters, ders, inverters, batteries, ...)."""
    section = profile[table]
    if isinstance(section, list):
        yield from section
        return
    for group in section.values():
        if isinstance(group, list):
            yield from group


def set_point(profile: dict, table: str, index: int, **fields):
    for point in _points(profile, table):
        if int(point.get('point_index', -1)) == index:
            point.update(fields)
            return point
    raise KeyError(f'{table} {index} not in profile')


def get_point(profile: dict, table: str, index: int) -> dict:
    for point in _points(profile, table):
        if int(point.get('point_index', -1)) == index:
            return point
    raise KeyError(f'{table} {index} not in profile')


class _Container:
    def __init__(self, name: str, profile: dict, work: Path, log_level: str = 'debug'):
        self.name, self.work, self.log_level = name, Path(work), log_level
        self.work.mkdir(parents=True, exist_ok=True)
        self.profile_path = self.work / f'{name}.profile.json'
        self.profile_path.write_text(json.dumps(profile))

    def _run(self, binary: str, *args: str):
        subprocess.run(['docker', 'rm', '-f', self.name], capture_output=True)
        cmd = ['docker', 'run', '-d', '--name', self.name, '--network', 'host', '-v', f'{CACHE_VOLUME}:/t:ro',
               '-v', f'{self.work}:/work:ro', IMAGE, f'/t/debug/{binary}', '--profile', f'/work/{self.profile_path.name}',
               '--log-level', self.log_level, *args]
        subprocess.run(cmd, check=True, capture_output=True)

    def logs(self) -> str:
        done = subprocess.run(['docker', 'logs', self.name], capture_output=True, text=True)
        return ANSI.sub('', done.stdout + done.stderr)

    def wait_for_log(self, pattern: str, timeout: float = 30.0, since: int = 0) -> str | None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            text = self.logs()[since:]
            for line in text.splitlines():
                if re.search(pattern, line):
                    return line
            time.sleep(0.5)
        return None

    def stop(self):
        subprocess.run(['docker', 'rm', '-f', self.name], capture_output=True)


class ReferenceOutstation(_Container):
    """The tool's outstation on ``127.0.0.1:port``: the external DNP3 device our master polls and operates."""
    def __init__(self, work: Path, port: int, profile: dict, outstation_address: int = 1, master_address: int = 2, name='e2e-dnp3-outstation',
                 log_level: str = 'debug'):
        super().__init__(name, profile, work, log_level)
        self.port, self.outstation_address, self.master_address = port, outstation_address, master_address

    def start(self, timeout: float = 30.0):
        self._run('reference-outstation', '--local', f'127.0.0.1:{self.port}', '--outstation-address', str(self.outstation_address),
                  '--master-address', str(self.master_address), '--conformance-loop-interval', '3600')
        deadline = time.time() + timeout
        import socket
        while time.time() < deadline:
            try:
                socket.create_connection(('127.0.0.1', self.port), timeout=0.5).close()
                return self
            except OSError:
                time.sleep(0.3)
        raise RuntimeError(f'reference outstation did not listen on {self.port}:\n{self.logs()[-2000:]}')


class ReferenceControlStation(_Container):
    """The tool's master against the outstation our platform serves on ``127.0.0.1:port``."""
    def __init__(self, work: Path, port: int, profile: dict, outstation_address: int = 1, master_address: int = 2,
                 control_loop_interval: int = 5, name='e2e-dnp3-master'):
        super().__init__(name, profile, work)
        self.port, self.outstation_address, self.master_address, self.interval = port, outstation_address, master_address, control_loop_interval

    def start(self):
        self._run('reference-control-station', '--outstation-ip', '127.0.0.1', '--outstation-port', str(self.port),
                  '--outstation-address', str(self.outstation_address), '--control-station-address', str(self.master_address),
                  '--control-loop-interval', str(self.interval), '--conformance-loop-interval', str(self.interval))
        return self

    def conformance_results(self) -> list[dict]:
        results = []
        for line in self.logs().splitlines():
            if 'MESA_CONFORMANCE_EVENT:' in line:
                try:
                    results.append(json.loads(line.split('MESA_CONFORMANCE_EVENT:', 1)[1]))
                except json.JSONDecodeError:
                    pass
        return results


def bounded(profile: dict, table: str, index: int, value: float, tolerance: float = 0.5) -> dict:
    """Narrow a point's profile range to ``value`` +/- ``tolerance`` so the control station's range checks and MON_001
    conformance test pass only when it read that value. Integer bounds stay integers (then +/- 1)."""
    point = get_point(profile, table, index)
    if isinstance(point.get('minimum', 0), int) and isinstance(point.get('maximum', 0), int):
        value = int(value) if float(value).is_integer() else value
        return set_point(profile, table, index, value=value, minimum=int(math.floor(value - tolerance)), maximum=int(math.ceil(value + tolerance)))
    return set_point(profile, table, index, value=value, minimum=value - tolerance, maximum=value + tolerance)


def widen(profile: dict, index: int, value: float):
    """Set an analog input's value, widening its range to include it; the profile's integer bounds stay integers."""
    point = get_point(profile, 'AI', index)
    low, high = point.get('minimum', 0), point.get('maximum', 0)
    cast = int if isinstance(low, int) and isinstance(high, int) and float(value).is_integer() else float
    set_point(profile, 'AI', index, value=cast(value) if cast is int else value,
              minimum=cast(min(low, value - 1)), maximum=cast(max(high, value + 1)))


def with_stimulus(profile: dict, analog_values: dict[int, float] | None = None, bounds: dict[int, float] | None = None,
                  ao_targets: dict[int, float] | None = None, quiet: bool = False) -> dict:
    """A copy of the profile: analog input ``value``s (what the reference outstation serves), input bounds (what the
    control station must read), and analog output targets (the ``value`` of the AO's associated AI, which the control
    station writes to the AO in engineering units). ``quiet`` removes every other analog input value, so a control
    station writes only the targeted analog outputs instead of every output whose input has a profile value."""
    p = copy.deepcopy(profile)
    if quiet:
        for point in _points(p, 'AI'):
            point.pop('value', None)
    for index, value in (analog_values or {}).items():
        widen(p, index, value)                                  # the profile validator requires minimum <= value <= maximum
    for index, value in (bounds or {}).items():
        bounded(p, 'AI', index, value)
    for index, value in (ao_targets or {}).items():
        ai = get_point(p, 'AO', index).get('assoc_ai')
        if not ai:
            raise KeyError(f'AO {index} has no associated AI in the profile')
        widen(p, int(re.sub(r'\D', '', ai)), value)
    return p


def trimmed(profile: dict, ai=(), ao=(), bi=(), bo=()) -> dict:
    """A copy of the profile keeping only the listed point indices per table (the tool accepts partial profiles). A
    control station on an AI-only profile reads and never writes; one with AO 87 / BO 17 writes just those."""
    p = copy.deepcopy(profile)
    for table, indices in (('AI', set(ai)), ('AO', set(ao)), ('BI', set(bi)), ('BO', set(bo))):
        section = p[table]
        if isinstance(section, list):
            p[table] = [pt for pt in section if int(pt.get('point_index', -1)) in indices]
        else:
            p[table] = {k: ([pt for pt in v if int(pt.get('point_index', -1)) in indices] if isinstance(v, list) else v) for k, v in section.items()}
    return p


def impossible(profile: dict, indices) -> dict:
    """Give analog inputs a [0, 1] range (value 0) so every real value the control station reads is reported as a
    range warning carrying the value: the control station's only output of what it read."""
    for index in indices:
        set_point(profile, 'AI', index, value=0, minimum=0, maximum=1)
    return profile


RANGE_WARNING = re.compile(r'Profile conformance: AI (\d+) value (-?[\d.]+) out of range')


def read_values(logs: str) -> dict[int, list[float]]:
    """Analog input values the control station read, from its range warnings (see ``impossible``), per index in order."""
    out: dict[int, list[float]] = {}
    for m in RANGE_WARNING.finditer(logs):
        out.setdefault(int(m.group(1)), []).append(float(m.group(2)))
    return out
