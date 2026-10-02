"""JSON replies for the DNP3 proxy: ``{"result": ..., "error": ...}`` as bytes, whatever the client returned."""
import json
import logging
from datetime import datetime
from enum import Enum

_log = logging.getLogger(__name__)


def jsonable(val):
    """Recursively convert a value into JSON-serializable primitives.

    Dict keys become strings, enums their names, datetimes ISO 8601 text, bytes hex; anything else is rendered with
    str() so an unexpected object never prevents a reply.
    """
    if val is None or isinstance(val, (str, int, float, bool)):
        return val
    if isinstance(val, dict):
        return {str(k): jsonable(v) for k, v in val.items()}
    if isinstance(val, (list, tuple, set, frozenset)):
        return [jsonable(v) for v in val]
    if isinstance(val, Enum):
        return val.name
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, (bytes, bytearray)):
        return val.hex()
    return str(val)


def serialize(result, error=None) -> bytes:
    """Encode a reply. ``error`` defaults to an empty mapping."""
    try:
        body = {'result': jsonable(result), 'error': jsonable(error if error is not None else {})}
    except Exception as e:    # pragma: no cover - jsonable() falls back to str(), so this is defensive only
        _log.exception('Unable to serialize a DNP3 proxy reply')
        body = {'result': {}, 'error': {'error': 'SerializationError', 'details': str(e)}}
    return json.dumps(body).encode('utf8')
