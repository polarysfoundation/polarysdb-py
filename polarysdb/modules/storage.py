"""
polarysdb.modules.storage
Storage engine with AES-256-GCM encryption, CRC32 integrity checks,
and atomic file writes — format-compatible with the Go implementation.

File layout (binary):
  Go-compatible (current default):
    [12 bytes] AES-GCM nonce
    [N bytes]  AES-256-GCM ciphertext  →  decrypts to UTF-8 JSON

  Legacy Python v1 (still readable for migration):
    [4 bytes]  magic number  "PLRD"
    [4 bytes]  version       0x00000001
    [4 bytes]  CRC32 of payload (before encryption)
    [12 bytes] AES-GCM nonce
    [N bytes]  AES-256-GCM ciphertext  →  decrypts to UTF-8 JSON
"""

from __future__ import annotations

import base64
import json
import os
import struct
import tempfile
import zlib
from pathlib import Path
from typing import Any

from .common import Key
from .crypto import decrypt, encrypt

MAGIC = b"PLRD"  # 4 bytes
VERSION: bytes = (1).to_bytes(4, "big")
HEADER_SIZE: int = 4 + 4 + 4 + 12  # magic + version + crc32 + nonce
NONCE_SIZE = 12


class Config:
    def __init__(
        self,
        data_path: str,
        encryption_key: Key,
        compression: bool = False,
        max_retries: int = 3,
        retry_delay: float = 0.1,
    ):
        self.data_path = data_path
        self.encryption_key = encryption_key
        self.compression = compression
        self.max_retries = max_retries
        self.retry_delay = retry_delay


class Engine:
    """
    Storage engine mirroring the Go storage.Engine.
    Handles encryption, serialization, and atomic file writes.
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._path: str = cfg.data_path
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_path(self) -> str:
        return self._path

    def update_key(self, new_key: Key) -> None:
        self.cfg.encryption_key = new_key

    def save(self, data: dict[str, dict[str, Any]]) -> None:
        """Serialize and persist data to disk atomically."""
        raw: bytes = self.serialize(data)
        self._atomic_write(self._path, raw)

    def load(self) -> tuple[dict[str, dict[str, Any]], float]:
        """
        Load and decrypt data from disk.
        Returns (data, mtime_float).
        """
        if not os.path.exists(self._path):
            return {}, 0.0

        with open(self._path, "rb") as f:
            raw: bytes = f.read()

        mtime: float = os.path.getmtime(self._path)
        data = self.deserialize(raw)
        return data, mtime

    def serialize(self, data: dict[str, dict[str, Any]]) -> bytes:
        """
        Encode data → encrypted binary payload.

        Default is Go-compatible encoding: nonce||ciphertext.
        """
        payload: bytes = json.dumps(
            _normalize_for_json(data), separators=(",", ":")
        ).encode("utf-8")
        key_bytes: bytes | None = (
            self.cfg.encryption_key.bytes() if self.cfg.encryption_key else None
        )
        return encrypt(payload, key_bytes)

    def deserialize(self, raw: bytes) -> dict[str, dict[str, Any]]:
        """
        Decrypt and decode binary payload → data dict.

        Supports:
          - Go-compatible format (nonce||ciphertext)
          - Legacy Python v1 (PLRD header) for migration
        """
        if len(raw) < NONCE_SIZE + 16:
            raise ValueError("file too short — not a valid PolarysDB file")

        # Legacy format detection
        if raw[:4] == MAGIC:
            return self._deserialize_legacy(raw)

        key_bytes = self.cfg.encryption_key.bytes()
        try:
            payload: bytes = decrypt(raw, key_bytes)
        except Exception as exc:
            raise ValueError("decryption failed — wrong key or corrupted data") from exc

        return json.loads(payload.decode("utf-8"))

    def _deserialize_legacy(self, raw: bytes) -> dict[str, dict[str, Any]]:
        if len(raw) < HEADER_SIZE:
            raise ValueError("file too short — not a valid legacy PolarysDB file")
        crc_stored = struct.unpack(">I", raw[8:12])[0]

        # En el formato legacy, la data cifrada empieza tras el header de 24 bytes
        encrypted_data: bytes = raw[12:]  # Nonce (12B) + Ciphertext

        key_bytes: bytes | None = self.cfg.encryption_key.bytes() if self.cfg.encryption_key else None
        payload: bytes = decrypt(encrypted_data, key_bytes)

        crc_actual: int = zlib.crc32(payload) & 0xFFFFFFFF
        if crc_stored != crc_actual:
            raise ValueError(
                f"CRC32 mismatch: stored={crc_stored:#010x} actual={crc_actual:#010x}"
            )

        return json.loads(payload.decode("utf-8"))

    # ------------------------------------------------------------------
    # Export / Import
    # ------------------------------------------------------------------

    def export_plain(self, data: dict[str, dict[str, Any]], path: str) -> None:
        """Export database as plain JSON (cross-language compatible)."""
        content: bytes = json.dumps(_normalize_for_json(data), indent=2).encode("utf-8")
        self._atomic_write(path, content)

    def import_plain(self, path: str) -> dict[str, dict[str, Any]]:
        """Import database from plain JSON."""
        with open(path, "rb") as f:
            return json.loads(f.read().decode("utf-8"))

    def export_encrypted(self, data: dict[str, dict[str, Any]], path: str) -> None:
        """Export database in encrypted binary format."""
        raw: bytes = self.serialize(data)
        self._atomic_write(path, raw)

    def import_encrypted(self, path: str) -> dict[str, dict[str, Any]]:
        """Import database from encrypted binary format."""
        with open(path, "rb") as f:
            raw: bytes = f.read()
        return self.deserialize(raw)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _atomic_write(self, path: str, data: bytes) -> None:
        """Write data atomically using a temp file + rename."""
        dir_name: str = os.path.dirname(os.path.abspath(path)) or "."
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, prefix=".pdb_tmp_")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


def _normalize_for_json(obj: Any) -> Any:
    """
    Normalize Python objects into JSON-compatible shapes matching Go encoding/json.

    - bytes/bytearray are encoded as base64 strings (same as Go's []byte).
    - dict keys must be strings (as in Go map[string]any).
    - Only JSON primitives + lists + dicts are allowed after normalization.
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (bytes, bytearray)):
        return base64.b64encode(bytes(obj)).decode("ascii")
    if isinstance(obj, list):
        return [_normalize_for_json(v) for v in obj]
    if isinstance(obj, tuple):
        return [_normalize_for_json(v) for v in obj]
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if not isinstance(k, str):
                raise TypeError("only string keys are supported in PolarysDB records")
            out[k] = _normalize_for_json(v)
        return out
    raise TypeError(f"value of type {type(obj)} is not JSON-serializable")
