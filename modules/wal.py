"""
polarysdb.modules.wal
Write-Ahead Log (WAL) implementation using a compact binary framing
that is structurally compatible with the Go protobuf-based WAL.

Frame layout per entry (length-prefixed):
  [4 bytes big-endian]  frame length (of the JSON payload)
  [N bytes]             JSON-encoded WAL entry
  [4 bytes big-endian]  CRC32 of the JSON payload
"""

import json
import os
import struct
import threading
import time
import zlib
from dataclasses import dataclass, field
from enum import IntEnum
from pathlib import Path
from queue import Queue, Empty
from typing import Any, List, Optional

from .logger import Logger


class OpType(IntEnum):
    CREATE = 0
    WRITE  = 1
    DELETE = 2


OP_CREATE = OpType.CREATE
OP_WRITE  = OpType.WRITE
OP_DELETE = OpType.DELETE


@dataclass
class Entry:
    op_type: OpType
    table: str
    key: str = ""
    value: Any = None
    timestamp: float = field(default_factory=time.time)


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

    FRAME_HEADER = struct.Struct(">I")   # 4-byte big-endian uint32
    FRAME_CRC    = struct.Struct(">I")

    def __init__(self, cfg: Config, logger: Optional[Logger] = None):
        self.cfg = cfg
        self._logger = logger
        self._path = cfg.path
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._pending: List[Entry] = []
        self._queue: Queue = Queue()

        # Open file in append+binary mode
        self._file = open(self._path, "ab")

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

        self._file.flush()
        os.fsync(self._file.fileno())

    def read_all(self) -> List[Entry]:
        """Read and return all WAL entries from disk."""
        if not os.path.exists(self._path):
            return []

        entries: List[Entry] = []
        with open(self._path, "rb") as f:
            while True:
                hdr = f.read(4)
                if not hdr:
                    break
                if len(hdr) < 4:
                    break

                (length,) = self.FRAME_HEADER.unpack(hdr)
                payload = f.read(length)
                crc_bytes = f.read(4)

                if len(payload) < length or len(crc_bytes) < 4:
                    break  # truncated entry — stop recovery

                (stored_crc,) = self.FRAME_CRC.unpack(crc_bytes)
                actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
                if stored_crc != actual_crc:
                    break  # corrupted entry — stop recovery

                try:
                    obj = json.loads(payload.decode("utf-8"))
                    entry = Entry(
                        op_type=OpType(obj["op_type"]),
                        table=obj["table"],
                        key=obj.get("key", ""),
                        value=obj.get("value"),
                        timestamp=obj.get("timestamp", 0.0),
                    )
                    entries.append(entry)
                except Exception:
                    break

        return entries

    def truncate(self) -> None:
        """Truncate the WAL file after a successful checkpoint."""
        self._file.close()
        open(self._path, "wb").close()
        self._file = open(self._path, "ab")

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
        obj = {
            "op_type":   int(entry.op_type),
            "table":     entry.table,
            "key":       entry.key,
            "value":     entry.value,
            "timestamp": entry.timestamp,
        }
        payload = json.dumps(obj, default=str).encode("utf-8")
        crc = zlib.crc32(payload) & 0xFFFFFFFF

        self._file.write(self.FRAME_HEADER.pack(len(payload)))
        self._file.write(payload)
        self._file.write(self.FRAME_CRC.pack(crc))
