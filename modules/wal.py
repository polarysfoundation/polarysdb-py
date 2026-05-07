"""
polarysdb.modules.wal
Write-Ahead Log (WAL) implementation compatible with the Go WAL:
  github.com/polarysfoundation/polarysdb/modules/wal

Go framing per entry:
  [4 bytes little-endian]  payload length
  [4 bytes little-endian]  CRC32(IEEE) of payload
  [N bytes]                protobuf WALEntry (proto3)
"""

from __future__ import annotations

import json
import os
import struct
import threading
import time
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .logger import Logger
from .protobuf_wire import (
    WIRE_LEN,
    WIRE_VARINT,
    decode_varint,
    encode_len,
    encode_key,
    encode_uvarint,
    encode_varint,
    parse_string_map_entry,
    skip_field,
)


OP_CREATE = 1
OP_WRITE = 2
OP_DELETE = 3
OP_COMMIT = 4


@dataclass
class Entry:
    op_type: int
    table: str
    key: str = ""
    value: Any = None
    tx_id: str = ""
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class Config:
    path: str
    sync_interval: float = 1.0   # seconds
    max_size: int = 100 * 1024 * 1024  # 100 MB


class WAL:
    """
    Write-Ahead Log — buffers entries in memory and flushes them to disk
    periodically, matching the Go wal.WAL interface.
    """

    FRAME_HEADER = struct.Struct("<I")   # 4-byte little-endian uint32
    FRAME_CRC = struct.Struct("<I")

    def __init__(self, cfg: Config, logger: Optional[Logger] = None):
        self.cfg = cfg
        self._logger = logger
        self._path = cfg.path
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._pending: List[Entry] = []

        # Open file in append+binary mode (create if missing)
        self._file = open(self._path, "ab", buffering=0)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def append(self, entry: Entry) -> None:
        """Buffer a WAL entry (non-blocking)."""
        with self._lock:
            self._pending.append(entry)

    def flush(self) -> None:
        """Flush all buffered entries to disk."""
        with self._lock:
            if not self._pending:
                return
            to_write = self._pending[:]
            self._pending.clear()

        for entry in to_write:
            self._write_frame(entry)

        os.fsync(self._file.fileno())

    def read_all(self) -> List[Entry]:
        """Read and return all WAL entries from disk."""
        if not os.path.exists(self._path):
            return []

        entries: List[Entry] = []
        with open(self._path, "rb") as f:
            while True:
                hdr = f.read(8)
                if not hdr:
                    break
                if len(hdr) < 8:
                    break

                try:
                    length = self.FRAME_HEADER.unpack(hdr[:4])[0]
                    stored_crc = self.FRAME_CRC.unpack(hdr[4:8])[0]
                except Exception:
                    break

                if length <= 0 or length > 10 * 1024 * 1024:
                    # Invalid / corrupted entry; stop recovery like Go read loop does
                    break

                payload = f.read(length)
                if len(payload) < length:
                    break

                actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
                if stored_crc != actual_crc:
                    # Corrupted entry; stop recovery
                    break

                try:
                    entries.append(_decode_wal_entry(payload))
                except Exception:
                    # Corrupted protobuf; stop recovery
                    break

        return entries

    def truncate(self) -> None:
        """Truncate the WAL file after a successful checkpoint."""
        self._file.close()
        open(self._path, "wb").close()
        self._file = open(self._path, "ab", buffering=0)

    def close(self) -> None:
        """Flush pending entries and close the WAL file."""
        self.flush()
        self._file.close()

    # ------------------------------------------------------------------
    # Background sync loop (called from thread)
    # ------------------------------------------------------------------

    def start(self, stop_event: threading.Event) -> None:
        """
        Background sync loop. Call in a dedicated daemon thread.
        Stops when stop_event is set.
        """
        while not stop_event.is_set():
            stop_event.wait(self.cfg.sync_interval)
            try:
                self.flush()
            except Exception as exc:
                if self._logger:
                    self._logger.warnf("WAL sync error: %s", exc)

        # Final flush before exit
        try:
            self.flush()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _write_frame(self, entry: Entry) -> None:
        payload = _encode_wal_entry(entry)
        crc = zlib.crc32(payload) & 0xFFFFFFFF

        header = self.FRAME_HEADER.pack(len(payload)) + self.FRAME_CRC.pack(crc)
        self._file.write(header)
        self._file.write(payload)


def _encode_wal_entry(entry: Entry) -> bytes:
    """
    Encode a WALEntry message compatible with Go wal.proto:
      uint32 op_type = 1;
      string table = 2;
      string key = 3;
      bytes value = 4;
      string tx_id = 5;
      int64 timestamp = 6;
      map<string, string> metadata = 7;
    """
    out = bytearray()
    if entry.op_type:
        out += encode_uvarint(1, int(entry.op_type))
    if entry.table:
        out += encode_len(2, entry.table.encode("utf-8"))
    if entry.key:
        out += encode_len(3, entry.key.encode("utf-8"))
    if entry.value is not None:
        out += encode_len(4, serialize_value(entry.value))
    if entry.tx_id:
        out += encode_len(5, entry.tx_id.encode("utf-8"))
    # Go uses nanoseconds since epoch (int64). Our Entry.timestamp is float seconds.
    ts_ns = int(entry.timestamp * 1_000_000_000)
    if ts_ns:
        out += encode_key(6, WIRE_VARINT) + encode_varint(ts_ns)
    if entry.metadata:
        for k, v in entry.metadata.items():
            me = bytearray()
            me += encode_len(1, str(k).encode("utf-8"))
            me += encode_len(2, str(v).encode("utf-8"))
            out += encode_len(7, bytes(me))
    return bytes(out)


def _decode_wal_entry(buf: bytes) -> Entry:
    pos = 0
    op_type = 0
    table = ""
    key = ""
    value: Any = None
    tx_id = ""
    timestamp_ns = 0
    metadata: Dict[str, str] = {}

    while pos < len(buf):
        tag, pos = decode_varint(buf, pos)
        field_no = tag >> 3
        wire = tag & 0x7

        if field_no in (1, 6) and wire == WIRE_VARINT:
            n, pos = decode_varint(buf, pos)
            if field_no == 1:
                op_type = int(n)
            else:
                timestamp_ns = int(n)
            continue

        if wire == WIRE_LEN:
            ln, pos = decode_varint(buf, pos)
            end = pos + ln
            if end > len(buf):
                raise ValueError("truncated length-delimited field")
            payload = buf[pos:end]
            if field_no == 2:
                table = payload.decode("utf-8", errors="replace")
            elif field_no == 3:
                key = payload.decode("utf-8", errors="replace")
            elif field_no == 4:
                value = deserialize_value(payload)
            elif field_no == 5:
                tx_id = payload.decode("utf-8", errors="replace")
            elif field_no == 7:
                me = parse_string_map_entry(payload)
                if me.key:
                    metadata[me.key] = me.value
            pos = end
            continue

        pos = skip_field(buf, pos, wire)

    ts = (timestamp_ns / 1_000_000_000) if timestamp_ns else 0.0
    return Entry(
        op_type=op_type,
        table=table,
        key=key,
        value=value,
        tx_id=tx_id,
        timestamp=ts or time.time(),
        metadata=metadata,
    )


def serialize_value(value: Any) -> bytes:
    """
    Mirrors Go wal.serializeValue() behavior:
      - bytes -> bytes
      - str -> utf-8 bytes
      - numbers/bool -> ascii bytes of fmt.Sprintf("%v", v)
      - complex types -> JSON
    """
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, bool):
        return (b"true" if value else b"false")
    if isinstance(value, (int, float)):
        return str(value).encode("utf-8")
    # Complex: JSON
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def deserialize_value(data: bytes) -> Any:
    """
    Mirrors Go wal.deserializeValue():
      - try JSON first
      - otherwise return utf-8 string
    """
    if not data:
        return None
    try:
        return json.loads(data.decode("utf-8"))
    except Exception:
        return data.decode("utf-8", errors="replace")
