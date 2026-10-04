"""The DNP3 protocol proxy: a process holding dnp3py masters for any number of outstations.

Everything about an outstation arrives in messages (REGISTER_OUTSTATION), so the proxy takes no launch options. Reads
and writes are keyed by the full point topics the caller registered, and unsolicited values are pushed back to the
manager as RECEIVE_UNSOLICITED, keyed the same way.
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

_log = logging.getLogger(__name__)

IDENTITY_FIELDS = ('host', 'port', 'master_address', 'outstation_address')
CONNECTION_FIELDS = ('response_timeout', 'link_reset', 'integrity_poll_interval', 'unsolicited', 'unsolicited_classes')


class Dnp3Proxy(AsyncioProtocolProxy):
    """One proxy process serves many outstations; each has its own master, connection and point table."""

    DEFAULT_CALLBACK_TIMEOUT = 120.0

    def __init__(self, callback_timeout: float = DEFAULT_CALLBACK_TIMEOUT, **kwargs):
        super(Dnp3Proxy, self).__init__(**kwargs)
        self.clients: dict[str, OutstationClient] = {}
        self.loop = asyncio.get_event_loop()
        self.register_callback(self.register_outstation_endpoint, 'REGISTER_OUTSTATION', provides_response=True,
                               timeout=callback_timeout)
        self.register_callback(self.read_points_endpoint, 'READ_POINTS', provides_response=True, timeout=callback_timeout)
        self.register_callback(self.write_points_endpoint, 'WRITE_POINTS', provides_response=True, timeout=callback_timeout)
        self.register_callback(self.close_outstation_endpoint, 'CLOSE_OUTSTATION', provides_response=True,
                               timeout=callback_timeout)

    # ---- helpers ------------------------------------------------------------------------------------------------
    @staticmethod
    def client_key(message: dict) -> str:
        return (f"{message.get('host')}:{message.get('port', 20000)}:{message.get('outstation_address', 1)}"
                f":{message.get('master_address', 2)}")

    def _get_client(self, message: dict, action: str) -> OutstationClient | None:
        client = self.clients.get(self.client_key(message))
        if client is None:
            _log.warning(f"Failed to {action}: outstation {self.client_key(message)} is not registered.")
        return client

    @staticmethod
    def _decode(raw_message: bytes) -> dict | None:
        try:
            message = json.loads(raw_message.decode('utf8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            _log.warning(f'Received undecodable request: {e}')
            return None
        return message if isinstance(message, dict) else None

    async def _push(self, client: OutstationClient, values: dict[str, Any]):
        """Send unsolicited values to the manager, fire-and-forget, keyed by the registered topics and tagged with the
        outstation's remote id so the manager routes them to the caller that registered it."""
        peer = self.peers.get(self.manager)
        if peer is None:    # pragma: no cover - the manager peer is created by the base constructor
            return
        await self.send(peer, ProtocolProxyMessage(method_name='RECEIVE_UNSOLICITED', payload=serialize(values),
                                                   remote_id=client.remote_id))

    @staticmethod
    def _remote_id(message: dict) -> UUID | None:
        value = message.get('remote_id')
        try:
            return UUID(str(value)) if value else None
        except ValueError:
            _log.warning(f'Ignoring malformed remote_id {value!r}')
            return None

    # ---- endpoints ----------------------------------------------------------------------------------------------
    @callback
    async def register_outstation_endpoint(self, _, raw_message: bytes):
        """Create or reuse the client for an outstation and replace its point table.

        Payload: ``host``, ``port``, ``master_address``, ``outstation_address``, the connection fields
        (``response_timeout``, ``link_reset``, ``integrity_poll_interval``, ``unsolicited``, ``unsolicited_classes``)
        and ``points``, a list of ``{topic, group, variation, index, scaling, control_code, count, on_time, off_time}``.
        Reply: ``{'client': key, 'points': n}``.
        """
        message = self._decode(raw_message)
        if message is None or not message.get('host'):
            return serialize({}, {'outstation': 'REGISTER_OUTSTATION needs at least a host.'})
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
            client.remote_id = self._remote_id(message)
            # A bad point table rejects the whole registration: the client is kept only once its points are good.
            count = client.configure_points(message.get('points') or [])
            self.clients[key] = client
            client.start()
            return serialize({'client': key, 'points': count})
        except (KeyError, TypeError, ValueError) as e:
            _log.warning(f'Failed to register outstation {key}: {e}')
            return serialize({}, {'outstation': str(e)})

    @callback
    async def read_points_endpoint(self, _, raw_message: bytes):
        """Read the outstation (``mode``: class, integrity or points; ``classes`` for a class read) and reply with the
        values of ``topics``: ``result`` maps topic to ``{value, quality, online, timestamp}``."""
        message = self._decode(raw_message)
        if message is None or (client := self._get_client(message, 'read points')) is None:
            return serialize({}, {'outstation': 'Outstation not registered.'})
        results, errors = await client.read(message.get('topics') or list(client.points), mode=message.get('mode', 'class'),
                                            classes=message.get('classes') or (0,))
        return serialize(results, errors)

    @callback
    async def write_points_endpoint(self, _, raw_message: bytes):
        """Operate outputs: ``operations`` are ``{topic, value}``, ``control_mode`` is ``direct`` or ``sbo``."""
        message = self._decode(raw_message)
        if message is None or (client := self._get_client(message, 'write points')) is None:
            return serialize({}, {'outstation': 'Outstation not registered.'})
        results, errors = await client.write(message.get('operations') or [], control_mode=message.get('control_mode', 'direct'))
        return serialize(results, errors)

    @callback
    async def close_outstation_endpoint(self, _, raw_message: bytes):
        message = self._decode(raw_message)
        if message is None:
            return serialize({}, {'outstation': 'Undecodable request.'})
        client = self.clients.pop(self.client_key(message), None)
        if client is not None:
            await client.close()
        return serialize({'closed': client is not None})

    @classmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        """Identify the proxy process a caller wants; all outstations share one unless the caller groups them."""
        return tuple(unique_remote_id)
