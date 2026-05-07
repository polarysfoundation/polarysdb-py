"""
polarysdb.modules.common
Common types and utilities shared across modules.
"""

import os
import hashlib
from typing import Optional

KEY_SIZE = 32  # 256-bit key


class Key:
    """
    Represents a 32-byte (256-bit) encryption key.
    Matches the Go common.Key type exactly.
    """

    SIZE = KEY_SIZE

    def __init__(self, data: Optional[bytes] = None):
        if data is None:
            self._bytes = bytes(KEY_SIZE)
        elif isinstance(data, (bytes, bytearray)):
            b = bytes(data)
            # Mirrors Go common.Key.SetBytes:
            # - if longer than 32, keep the last 32 bytes
            # - if shorter, left-pad with zeros
            if len(b) > KEY_SIZE:
                b = b[-KEY_SIZE:]
            if len(b) < KEY_SIZE:
                b = (b"\x00" * (KEY_SIZE - len(b))) + b
            self._bytes = b
        elif isinstance(data, str):
            s = data
            if s.startswith(("0x", "0X")):
                s = s[2:]
            encoded = s.encode("utf-8")
            # Same SetBytes behavior as above
            if len(encoded) > KEY_SIZE:
                encoded = encoded[-KEY_SIZE:]
            if len(encoded) < KEY_SIZE:
                encoded = (b"\x00" * (KEY_SIZE - len(encoded))) + encoded
            self._bytes = encoded
        else:
            raise TypeError(f"Key must be bytes, bytearray, or str, got {type(data)}")

    @classmethod
    def from_passphrase(cls, passphrase: str) -> "Key":
        """Derive a 32-byte key from a passphrase using SHA-256."""
        h = hashlib.sha256(passphrase.encode("utf-8")).digest()
        return cls(h)

    @classmethod
    def generate(cls) -> "Key":
        """Generate a secure random key."""
        return cls(os.urandom(KEY_SIZE))

    def bytes(self) -> bytes:
        return self._bytes

    def is_zero(self) -> bool:
        return self._bytes == bytes(KEY_SIZE)

    def __eq__(self, other) -> bool:
        if isinstance(other, Key):
            return self._bytes == other._bytes
        if isinstance(other, (bytes, bytearray)):
            return self._bytes == bytes(other)
        return False

    def __repr__(self) -> str:
        return f"Key({self._bytes.hex()[:8]}...)"

    def __hash__(self):
        return hash(self._bytes)


def is_equal(a: bytes, b: bytes) -> bool:
    """Constant-time comparison of two byte sequences."""
    if len(a) != len(b):
        return False
    result = 0
    for x, y in zip(a, b):
        result |= x ^ y
    return result == 0
