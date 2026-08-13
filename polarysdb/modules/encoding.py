import json
from typing import Any

# Constantes de Type Tags idénticas a Go
TAG_NIL = 0
TAG_BYTES = 1
TAG_STRING = 2
TAG_INT = 3
TAG_UINT = 4
TAG_FLOAT = 5
TAG_BOOL = 6
TAG_JSON = 7


def serialize_value(value: Any) -> bytes:
    """Serializa un valor agregando el primer byte de tag igual que Go."""
    if value is None:
        return bytes([TAG_NIL])

    if isinstance(value, (bytes, bytearray)):
        return bytes([TAG_BYTES]) + bytes(value)

    if isinstance(value, str):
        return bytes([TAG_STRING]) + value.encode("utf-8")

    if isinstance(value, bool):
        return bytes([TAG_BOOL, 1 if value else 0])

    if isinstance(value, int):
        # En Go los enteros se guardan formateados como string ASCII con tagInt/tagUint
        if value >= 0:
            return bytes([TAG_UINT]) + str(value).encode("utf-8")
        return bytes([TAG_INT]) + str(value).encode("utf-8")

    if isinstance(value, float):
        return bytes([TAG_FLOAT]) + str(value).encode("utf-8")

    # Por defecto (dict, list, u objetos complejos): JSON
    data = json.dumps(value, separators=(",", ":")).encode("utf-8")
    return bytes([TAG_JSON]) + data


def deserialize_value(data: bytes) -> Any:
    """Deserializa un valor leyendo el primer byte de tag igual que Go."""
    if not data:
        return None

    tag = data[0]
    payload = data[1:]

    if tag == TAG_NIL:
        return None

    if tag == TAG_BYTES:
        return bytes(payload)

    if tag == TAG_STRING:
        return payload.decode("utf-8", errors="replace")

    if tag == TAG_INT:
        return int(payload.decode("utf-8"))

    if tag == TAG_UINT:
        return int(payload.decode("utf-8"))

    if tag == TAG_FLOAT:
        return float(payload.decode("utf-8"))

    if tag == TAG_BOOL:
        if not payload:
            raise ValueError("invalid bool: empty payload")
        return payload[0] == 1

    if tag == TAG_JSON:
        return json.loads(payload.decode("utf-8"))

    raise ValueError(f"unknown type tag: {tag}")
