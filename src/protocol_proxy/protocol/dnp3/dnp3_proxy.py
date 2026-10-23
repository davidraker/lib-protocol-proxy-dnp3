"""The DNP3 protocol proxy: a process holding dnp3py masters for any number of outstations, and dnp3py outstations
served to remote masters, in either role as the registration says.

Everything about a remote arrives in messages (REGISTER_REMOTE, or its older name REGISTER_OUTSTATION), so the proxy
takes no launch options. ``role`` is ``master`` (the default; ``client`` is accepted) to reach an outstation, or
``outstation`` (``server``) to serve one. The registering caller's remote id (header version 2) identifies the remote in
every later message; the caller and the proxy are always installed together, so there is no payload fallback. Reads and
writes are keyed by the full point topics the caller registered and mean the same in both roles (what the proxy holds
for the point; store this value), and values the other side produced (unsolicited responses from an outstation, a
master's controls on a served outstation) are pushed back to the manager as RECEIVE_UNSOLICITED, tagged with the remote
id and keyed the same way. A served outstation also pushes its whole table when it is (re)registered.
"""
import asyncio
import json
import logging
from functools import partial
from typing import Any
from uuid import UUID

from protocol_proxy.ipc import callback, ProtocolProxyMessage
from protocol_proxy.proxy.asyncio import AsyncioProtocolProxy

from .client import OutstationClient
from .json import serialize
from .server import ServedOutstation

_log = logging.getLogger(__name__)

IDENTITY_FIELDS = ('host', 'port', 'master_address', 'outstation_address')
CONNECTION_FIELDS = ('response_timeout', 'link_reset', 'integrity_poll_interval', 'unsolicited', 'unsolicited_classes')
ROLES = {'master': 'master', 'client': 'master', 'outstation': 'outstation', 'server': 'outstation'}

Remote = OutstationClient | ServedOutstation


class Dnp3Proxy(AsyncioProtocolProxy):
    """One proxy process serves many outstations; each has its own master, connection and point table."""

    DEFAULT_CALLBACK_TIMEOUT = 120.0

    def __init__(self, callback_timeout: float = DEFAULT_CALLBACK_TIMEOUT, **kwargs):
        super(Dnp3Proxy, self).__init__(**kwargs)
        self.clients: dict[str, OutstationClient] = {}       # masters, by outstation address
        self.servers: dict[str, ServedOutstation] = {}       # served outstations, by listener and address
        self.remotes: dict[UUID, Remote] = {}                # either, by the caller's remote id (header version 2)
        self.loop = asyncio.get_event_loop()
        for name in ('REGISTER_REMOTE', 'REGISTER_OUTSTATION'):
            self.register_callback(self.register_outstation_endpoint, name, provides_response=True, timeout=callback_timeout)
        self.register_callback(self.read_points_endpoint, 'READ_POINTS', provides_response=True, timeout=callback_timeout)
        self.register_callback(self.write_points_endpoint, 'WRITE_POINTS', provides_response=True, timeout=callback_timeout)
        for name in ('CLOSE_REMOTE', 'CLOSE_OUTSTATION'):
            self.register_callback(self.close_outstation_endpoint, name, provides_response=True, timeout=callback_timeout)

    # ---- helpers ------------------------------------------------------------------------------------------------
    @staticmethod
    def client_key(message: dict) -> str:
        return (f"{message.get('host')}:{message.get('port', 20000)}:{message.get('outstation_address', 1)}"
                f":{message.get('master_address', 2)}")

    @staticmethod
    def server_key(message: dict) -> str:
        return (f"outstation:{message.get('host', '0.0.0.0')}:{message.get('port', 20000)}"
                f":{message.get('outstation_address', 1)}")

    def _get_client(self, headers, message: dict, action: str) -> Remote | None:
        """The remote (a master's outstation, or a served outstation) a request concerns, by its header's remote id."""
        remote_id = getattr(headers, 'remote_id', None)
        client = self.remotes.get(remote_id) if remote_id else None
        if client is None:
            _log.warning(f"Failed to {action}: remote {remote_id} has no registered outstation.")
        return client

    @staticmethod
    def _decode(raw_message: bytes) -> dict | None:
        try:
            message = json.loads(raw_message.decode('utf8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            _log.warning(f'Received undecodable request: {e}')
            return None
        return message if isinstance(message, dict) else None

    async def _push(self, client: Remote, values: dict[str, Any]):
        """Send values the other side produced to the manager, fire-and-forget, keyed by the registered topics and
        tagged with the remote's id so the manager routes them to the caller that registered it."""
        peer = self.peers.get(self.manager)
        if peer is None:    # pragma: no cover - the manager peer is created by the base constructor
            return
        await self.send(peer, ProtocolProxyMessage(method_name='RECEIVE_UNSOLICITED', payload=serialize(values),
                                                   remote_id=client.remote_id))

    @staticmethod
    def _remote_id(headers) -> UUID | None:
        """The caller's id for the outstation, from the header."""
        return getattr(headers, 'remote_id', None)

    # ---- endpoints ----------------------------------------------------------------------------------------------
    @callback
    async def register_outstation_endpoint(self, headers, raw_message: bytes):
        """Register a remote in either role and replace its point table (REGISTER_REMOTE / REGISTER_OUTSTATION).

        Payload: ``role`` (``master`` by default, or ``outstation``), ``host``, ``port``, ``master_address``,
        ``outstation_address``, the connection fields (``response_timeout``, ``link_reset``,
        ``integrity_poll_interval``, ``unsolicited``, ``unsolicited_classes``; a served outstation uses the last two)
        and ``points``, a list of ``{topic, group, variation, index, scaling, control_code, count, on_time, off_time}``
        plus, for a served outstation, ``event_class``, ``remote_writable`` and ``initial_value``; a served outstation
        may also get ``values`` (``{topic: value}``, the caller's current values), which seed new points ahead of
        their ``initial_value``; values the proxy already holds win. Reply: ``{'client': key, 'points': n}`` for a master, ``{'server': key, 'points': n, 'role':
        'outstation', 'seeded': m}`` for a served outstation, which also pushes its whole table.
        """
        message = self._decode(raw_message)
        if message is None:
            return serialize({}, {'outstation': 'Undecodable registration.'})
        role = ROLES.get(str(message.get('role') or 'master').lower())
        if role is None:
            return serialize({}, {'outstation': f"Unknown role {message.get('role')!r}; expected master or outstation."})
        remote_id = self._remote_id(headers)
        if remote_id is None:
            return serialize({}, {'outstation': 'Registration needs the remote id in its header (version 2).'})
        if role == 'outstation':
            return await self._register_served(remote_id, message)
        if not message.get('host'):
            return serialize({}, {'outstation': 'Registering an outstation to reach needs at least a host.'})
        key = self.client_key(message)
        try:
            client = self.clients.get(key)
            if client is None:
                connection = {k: message[k] for k in CONNECTION_FIELDS if k in message}
                client = OutstationClient(message['host'], int(message.get('port', 20000)),
                                          master_address=int(message.get('master_address', 2)),
                                          outstation_address=int(message.get('outstation_address', 1)), **connection)
                client.push = partial(self._push, client)
            else:
                for field in ('integrity_poll_interval', 'unsolicited', 'unsolicited_classes'):
                    if field in message:
                        setattr(client, field, set(message[field]) if field == 'unsolicited_classes' else message[field])
            client.remote_id = remote_id
            # A bad point table rejects the whole registration: the client is kept only once its points are good.
            count = client.configure_points(message.get('points') or [])
            self.clients[key] = client
            self.remotes[remote_id] = client
            client.start()
            return serialize({'client': key, 'points': count})
        except (KeyError, TypeError, ValueError) as e:
            _log.warning(f'Failed to register outstation {key}: {e}')
            return serialize({}, {'outstation': str(e)})

    async def _register_served(self, remote_id: UUID, message: dict):
        key = self.server_key(message)
        served = self.servers.get(key)
        try:
            if served is None:
                served = ServedOutstation(message.get('host', '0.0.0.0'), int(message.get('port', 20000)),
                                          outstation_address=int(message.get('outstation_address', 1)),
                                          master_address=int(message.get('master_address', 2)),
                                          unsolicited=bool(message.get('unsolicited', False)),
                                          unsolicited_classes=message.get('unsolicited_classes') or (1, 2, 3))
                served.push = partial(self._push, served)
            served.remote_id = remote_id
            count = served.configure_points(message.get('points') or [], message.get('values') or {})
            await served.start()
        except Exception as e:      # a bad row, or whatever the listener's bind raised (dnp3py's ChannelError, OSError)
            _log.warning(f'Failed to serve outstation {key}: {e}')
            if served is not None and key not in self.servers:
                await served.close()
            return serialize({}, {'outstation': str(e)})
        self.servers[key] = served
        self.remotes[remote_id] = served
        self.loop.create_task(served.push_all())        # the platform gets the whole served table
        return serialize({'server': key, 'points': count, 'role': 'outstation', 'seeded': served.seeded})

    @callback
    async def read_points_endpoint(self, headers, raw_message: bytes):
        """Read the outstation (``mode``: class, integrity or points; ``classes`` for a class read) and reply with the
        values of ``topics``: ``result`` maps topic to ``{value, quality, online, timestamp}``. For a served
        outstation the values are the ones it holds; ``mode`` and ``classes`` are ignored."""
        message = self._decode(raw_message)
        if message is None or (client := self._get_client(headers, message, 'read points')) is None:
            return serialize({}, {'outstation': 'Outstation not registered.'})
        results, errors = await client.read(message.get('topics') or list(client.points), mode=message.get('mode', 'class'),
                                            classes=message.get('classes') or (0,))
        return serialize(results, errors)

    @callback
    async def write_points_endpoint(self, headers, raw_message: bytes):
        """Operate outputs: ``operations`` are ``{topic, value}``, ``control_mode`` is ``direct`` or ``sbo``. For a
        served outstation the operations set its served values (any registered point) and ``control_mode`` is ignored."""
        message = self._decode(raw_message)
        if message is None or (client := self._get_client(headers, message, 'write points')) is None:
            return serialize({}, {'outstation': 'Outstation not registered.'})
        results, errors = await client.write(message.get('operations') or [], control_mode=message.get('control_mode', 'direct'))
        return serialize(results, errors)

    async def stop(self):
        """Close every master connection and served outstation before the IPC stops, so their streams and transports
        are released while the loop still runs (not reclaimed later with a ResourceWarning)."""
        for client in list({id(c): c for c in [*self.clients.values(), *self.servers.values()]}.values()):
            try:
                await client.close()
            except Exception as e:  # pragma: no cover
                _log.debug(f'Error closing a DNP3 remote on stop: {e}')
        self.clients.clear()
        self.servers.clear()
        self.remotes.clear()
        await super(Dnp3Proxy, self).stop()

    @callback
    async def close_outstation_endpoint(self, headers, raw_message: bytes):
        message = self._decode(raw_message)
        if message is None:
            return serialize({}, {'outstation': 'Undecodable request.'})
        client = self._get_client(headers, message, 'close outstation')
        if client is not None:
            self.clients = {k: c for k, c in self.clients.items() if c is not client}
            self.servers = {k: c for k, c in self.servers.items() if c is not client}
            self.remotes = {k: c for k, c in self.remotes.items() if c is not client}
            await client.close()
        return serialize({'closed': client is not None})

    @classmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        """Identify the proxy process a caller wants; all outstations share one unless the caller groups them."""
        return tuple(unique_remote_id)
