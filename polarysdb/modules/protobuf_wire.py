"""
Minimal Protocol Buffers (proto3) wire encoder/decoder utilities.

This is intentionally tiny: we only need varints and length-delimited fields to
encode/decode the Go WALEntry message bytes produced by proto.Marshal().
"""

from __future__ import annotations

from dataclasses import dataclass

WIRE_VARINT = 0
WIRE_LEN = 2


def encode_varint(value: int) -> bytes:
    if value < 0:
        # proto3 int64 uses two's complement varint encoding for negative numbers
        # (10 bytes). We only need non-negative for our use-cases.
        value &= (1 << 64) - 1
    out = bytearray()
    while True:
        b = value & 0x7F
        value >>= 7
        if value:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def decode_varint(buf: bytes, pos: int) -> tuple[int, int]:
    shift = 0
    result = 0
    while True:
        if pos >= len(buf):
            raise ValueError("truncated varint")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return result, pos
        shift += 7
        if shift > 70:
            raise ValueError("varint too long")


def encode_key(field_number: int, wire_type: int) -> bytes:
    return encode_varint((field_number << 3) | wire_type)


def encode_len(field_number: int, payload: bytes) -> bytes:
    return encode_key(field_number, WIRE_LEN) + encode_varint(len(payload)) + payload


def encode_uvarint(field_number: int, value: int) -> bytes:
    return encode_key(field_number, WIRE_VARINT) + encode_varint(value)


def skip_field(buf: bytes, pos: int, wire_type: int) -> int:
    if wire_type == WIRE_VARINT:
        _, pos = decode_varint(buf, pos)
        return pos
    if wire_type == WIRE_LEN:
        length, pos = decode_varint(buf, pos)
        end = pos + length
        if end > len(buf):
            raise ValueError("truncated length-delimited field")
        return end
    raise ValueError(f"unsupported wire type: {wire_type}")


@dataclass
class ParsedMapEntry:
    key: str = ""
    value: str = ""


def parse_string_map_entry(payload: bytes) -> ParsedMapEntry:
    """
    Parse map<string,string> entry message:
      message Entry { string key = 1; string value = 2; }
    """
    pos = 0
    out = ParsedMapEntry()
    while pos < len(payload):
        tag, pos = decode_varint(payload, pos)
        field_no = tag >> 3
        wire = tag & 0x7
        if wire != WIRE_LEN:
            pos = skip_field(payload, pos, wire)
            continue
        ln, pos = decode_varint(payload, pos)
        end = pos + ln
        if end > len(payload):
            raise ValueError("truncated map entry")
        s = payload[pos:end].decode("utf-8", errors="replace")
        if field_no == 1:
            out.key = s
        elif field_no == 2:
            out.value = s
        pos = end
    return out


def parse_string_map_field(buf: bytes) -> dict[str, str]:
    """
    Helper for repeated map-entry field payloads when caller already extracted them.
    """
    # Not used directly; kept for completeness.
    return {}

