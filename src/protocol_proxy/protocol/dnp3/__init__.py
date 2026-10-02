"""DNP3 (IEEE 1815) master plugin for protocol_proxy, built on dnp3py.

Proxies are launched with ``python -m protocol_proxy.proxy <module>:<class>`` (see protocol_proxy.proxy.launch), so
this package may import its proxy class directly. Dnp3Proxy declares no LAUNCHER: it takes no protocol-specific
launch options, since everything about an outstation arrives via REGISTER_OUTSTATION.
"""
from .client import OutstationClient, PointSpec
from .dnp3_proxy import Dnp3Proxy

__all__ = ['Dnp3Proxy', 'OutstationClient', 'PointSpec', 'PROXY_CLASS']

PROXY_CLASS = Dnp3Proxy
