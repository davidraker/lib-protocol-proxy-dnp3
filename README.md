# Protocol Proxy DNP3 Library

This library provides support for communication with DNP3 (IEEE 1815-2012) outstations to a
[Protocol Proxy](https://github.com/eclipse-volttron/lib-protocol-proxy) Manager. Communication happens in a separate
proxy process which holds a [dnp3py](https://github.com/craigpnnl/dnp3py) master for every outstation, polls on the
caller's behalf, operates outputs and forwards unsolicited responses. One proxy process serves any number of
outstations over TCP.

## Automatically installed dependencies
- python = ">=3.11,<4.0"
- protocol-proxy = ">=2.0.0rc3"
- dnp3py (pure Python, no C dependencies). Until a release carries analog output command variations, command status
  delivery and the runner's operate helpers, the dependency is the `volttron-dnp3` branch of the `davidraker/dnp3py`
  fork, which merges the three pull requests adding them.

# Installation

```shell
pip install protocol-proxy-dnp3
```

This is rarely necessary: the library is a dependency of an application acting as a Protocol Proxy Manager, such as the
VOLTTRON DNP3 Driver Interface (`volttron-lib-dnp3-driver`), and is installed with it.

# How it works

A Protocol Proxy Manager launches the proxy with:

```shell
python -m protocol_proxy.proxy protocol_proxy.protocol.dnp3.dnp3_proxy:Dnp3Proxy [manager options]
```

The DNP3 proxy takes no protocol-specific launch options. Everything about an outstation arrives in messages, and every
point is identified by the topic the caller registered it under:

| Message | Purpose |
|---|---|
| `REGISTER_OUTSTATION` | Create (or reuse) the master for an outstation and replace its point table. Carries `host`, `port` (20000), `master_address` (2), `outstation_address` (1), `response_timeout` (5 s), `link_reset` (true), `integrity_poll_interval` (3600 s; 0 disables the periodic integrity poll, one always runs on connect), `unsolicited` (false), `unsolicited_classes` ([1, 2, 3]) and `points`: a list of `{topic, group, variation, index, scaling, control_code, count, on_time, off_time}`. Registering again replaces the point table, so a caller re-registers after a configuration change. |
| `READ_POINTS` | Refresh the outstation and reply with the values of `topics`. `mode` is `class` (read the data classes in `classes`, default `[0]`, static data), `integrity` (Class 0, 1, 2 and 3) or `points` (one range read per contiguous index span of each group and variation named). Whatever the mode, the read becomes an integrity poll when `integrity_poll_interval` has elapsed. |
| `WRITE_POINTS` | Operate outputs. `operations` are `{topic, value}`; `control_mode` is `direct` (DIRECT_OPERATE) or `sbo` (SELECT then OPERATE, the OPERATE sent only when the SELECT echo accepts every control). Binary outputs take a boolean and send the point's `control_code` (`latch` on/off, or `pulse` on/off with its `count`, `on_time` and `off_time`); analog outputs take a number, divided by the point's `scaling`, in the group 41 variation matching the registered group 40 variation. |
| `CLOSE_OUTSTATION` | Close the connection and forget the outstation. |

Replies have the form `{"result": ..., "error": ...}`. For reads, `result` maps topic to `{value, quality, online,
timestamp}` with `scaling` applied (engineering value = raw value times scaling), and `error` maps a topic to its reason
(`unregistered topic`, `not reported by outstation`) or holds a single `link` entry when the exchange itself failed. For
writes, `result` maps topic to `{status: "SUCCESS", value}` and `error` carries the status the outstation echoed
(`status NOT_SUPPORTED`) or why nothing was sent.

## Unsolicited responses

With `unsolicited` true the proxy enables the named classes after its startup integrity poll, listens for events whenever
it is not busy with a request, confirms them, and sends the values to the manager as `RECEIVE_UNSOLICITED` with the
payload `{"result": {topic: value, ...}, "error": {}}`, scaled like a read. Only registered points are forwarded. The
listener also keeps the link up, reconnecting after a delay when it drops; without unsolicited reporting the connection
is reopened lazily on the next request.

## Connection and sequencing

Requests to one outstation share one connection and run one at a time; unsolicited listening is done in short slices so
a read or write waits at most about half a second for the link. On connect the proxy disables unsolicited reporting
(ignoring an outstation that refuses), runs an integrity poll, then re-enables the configured classes, following the
startup sequence of IEEE 1815-2012 5.1.6. A link failure closes the connection and is reported as a `link` error; the
next request reopens it.

Each message is allowed 120 seconds by default before being abandoned (`Dnp3Proxy(callback_timeout=...)`).

# Testing

```shell
pytest tests
```

The tests need no network beyond the loopback interface. They run dnp3py's own outstation on 127.0.0.1 (a plain
`Outstation` with a small database, and the MESA IEEE 1815.2 DER simulator shipped with dnp3py), drive the client
against it, exercise the unsolicited listener over dnp3py's in-memory channel pair, and call every endpoint through the
real IPC base class.

# Development
This library is maintained by the VOLTTRON Development Team.

Please see the following [guidelines](https://github.com/eclipse-volttron/volttron-core/blob/develop/CONTRIBUTING.md)
for contributing to this and/or other VOLTTRON repositories.

# Disclaimer Notice

This material was prepared as an account of work sponsored by an agency of the
United States Government.  Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in the development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or any
information, apparatus, product, software, or process disclosed, or represents
that its use would not infringe privately owned rights.

Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the United
States Government or any agency thereof, or Battelle Memorial Institute. The
views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.
