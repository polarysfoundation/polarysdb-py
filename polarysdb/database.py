"""
polarysdb.database
Core Database implementation — Python port of the Go polarysdb package.

Mirrors the Go API exactly:
  db = polarysdb.init(key, dir_path, debug)
  db = polarysdb.init_with_config(cfg)
  db.create(table)
  db.write(table, key, value)
  db.read(table, key) → (value, exists)
  db.delete(table, key)
  db.write_batch(table, records)
  db.read_batch(table) → [values]
  db.create_index(table, field)
  db.query_by_index(table, field, value) → [values]
  db.begin_transaction() → Transaction
  db.commit_transaction(tx)
  db.export(key, path)
  db.export_encrypted(key, path)
  db.import_db(key, path)
  db.import_encrypted(key, path)
  db.get_metrics() → MetricsSnapshot
  db.get_status() → dict
  db.close()
  db.close_with_timeout(seconds)
"""

import copy
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .modules.backup import Config as BackupConfig, Manager as BackupManager
from .modules.common import Key, is_equal
from .modules.config import get_state_db_path
from .modules.index import Manager as IndexManager
from .modules.logger import Config as LogConfig, Level, Logger
from .modules.metrics import Collector as MetricsCollector, Snapshot as MetricsSnapshot
from .modules.storage import Config as StorageConfig, Engine as StorageEngine
from .modules.tx import Manager as TxManager, Transaction
from .modules.wal import (
    Config as WALConfig,
    Entry as WALEntry,
    OP_CREATE,
    OP_DELETE,
    OP_WRITE,
    WAL,
)

MAX_BATCH_SIZE = 100_000
_WRITE_TIMEOUT = 5.0  # seconds


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class Config:
    # Paths
    dir_path: str = "./data"
    backup_dir: str = "./backups"

    # Security
    encryption_key: Key = field(default_factory=Key)

    # Features
    enable_wal: bool = True
    enable_backup: bool = True
    enable_indexes: bool = True
    enable_transactions: bool = True
    enable_compression: bool = False

    # Performance
    save_interval: float = 5.0  # seconds
    wal_sync_interval: float = 1.0  # seconds
    watch_interval: float = 3.0  # seconds
    buffer_size: int = 1000
    max_connections: int = 1000

    # Reliability
    max_retries: int = 3
    retry_delay: float = 0.1  # seconds
    backup_interval: float = 3600.0  # seconds

    # Monitoring
    debug: bool = False
    metrics_enabled: bool = True


def default_config() -> Config:
    return Config()


# ---------------------------------------------------------------------------
# Internal write operation
# ---------------------------------------------------------------------------


class _WriteOp:
    __slots__ = ("op_type", "table", "key", "value", "result")

    def __init__(self, op_type: str, table: str, key: str = "", value: Any = None):
        self.op_type = op_type
        self.table = table
        self.key = key
        self.value = value
        self.result: queue.Queue = queue.Queue(maxsize=1)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


class Database:
    """
    PolarysDB — Python edition.
    Thread-safe, embedded key-value database with encryption and WAL.
    """

    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._data: Dict[str, Dict[str, Any]] = {}
        self._data_lock = (
            threading.RWLock() if hasattr(threading, "RWLock") else _FairRWLock()
        )

        # Logger
        log_cfg = LogConfig(
            min_level=Level.DEBUG if cfg.debug else Level.INFO,
            to_console=True,
        )
        self._log = Logger(log_cfg)

        # Stop signal for background threads
        self._stop = threading.Event()
        self._closed = threading.Event()

        # Write buffer (async batching)
        self._write_buf: queue.Queue = queue.Queue(maxsize=cfg.buffer_size)

        # Dirty flag
        self._dirty = threading.Event()

        # Last loaded / saved timestamps
        self._last_loaded: float = 0.0
        self._last_save: float = 0.0

        # -- Storage engine --
        db_path = get_state_db_path(cfg.dir_path)
        self._storage = StorageEngine(
            StorageConfig(
                data_path=db_path,
                encryption_key=cfg.encryption_key,
                compression=cfg.enable_compression,
                max_retries=cfg.max_retries,
                retry_delay=cfg.retry_delay,
            )
        )

        # -- WAL --
        self._wal: Optional[WAL] = None
        if cfg.enable_wal:
            wal_path = os.path.join(cfg.dir_path, "polarysdb.wal")
            self._wal = WAL(
                WALConfig(
                    path=wal_path,
                    sync_interval=cfg.wal_sync_interval,
                ),
                self._log,
            )

        # -- Index manager --
        self._index: Optional[IndexManager] = None
        if cfg.enable_indexes:
            self._index = IndexManager(self._log)

        # -- Transaction manager --
        self._tx: Optional[TxManager] = None
        if cfg.enable_transactions:
            self._tx = TxManager(self._log)

        # -- Backup manager --
        self._backup: Optional[BackupManager] = None
        if cfg.enable_backup:
            self._backup = BackupManager(
                BackupConfig(
                    backup_dir=cfg.backup_dir,
                    interval=cfg.backup_interval,
                    keep_count=10,
                ),
                self._log,
            )

        # -- Metrics --
        self._metrics: Optional[MetricsCollector] = None
        if cfg.metrics_enabled:
            self._metrics = MetricsCollector()

        # Load persisted snapshot first
        self._load_with_retry()

        # WAL recovery after loading snapshot (Go-compatible semantics)
        recovered = 0
        if self._wal:
            try:
                recovered = self._recover_from_wal()
            except Exception as exc:
                self._log.warnf("WAL recovery failed: %s — continuing", exc)

        # Checkpoint: persist recovered state then truncate WAL
        if recovered > 0 and self._wal:
            try:
                self._flush_to_disk()
                self._wal.truncate()
            except Exception as exc:
                self._log.warnf("WAL checkpoint failed: %s", exc)

        # Start background workers
        self._threads: List[threading.Thread] = []
        self._start_background_workers()

        self._log.info("PolarysDB (Python) initialized successfully")

    # -----------------------------------------------------------------------
    # Table operations
    # -----------------------------------------------------------------------

    def exist(self, table: str) -> bool:
        """Return True if the table exists."""
        if self._closed.is_set():
            return False
        with self._data_lock.read():
            return table in self._data

    def create(self, table: str) -> None:
        """Create a new table (no-op if it already exists)."""
        self._send_op(_WriteOp("create", table))

    # -----------------------------------------------------------------------
    # Data operations
    # -----------------------------------------------------------------------

    def write(self, table: str, key: str, value: Any) -> None:
        """Write a record into a table."""
        t0 = time.monotonic()
        self._send_op(_WriteOp("write", table, key, value))
        if self._metrics:
            self._metrics.record_write_latency(time.monotonic() - t0)

    def write_batch(self, table: str, records: Dict[str, Any]) -> None:
        """Write multiple records at once (more efficient than individual writes)."""
        if self._closed.is_set():
            raise RuntimeError("database is closed")
        if len(records) > MAX_BATCH_SIZE:
            raise ValueError(
                f"batch size {len(records)} exceeds maximum {MAX_BATCH_SIZE}"
            )

        with self._data_lock.write():
            if table not in self._data:
                raise KeyError(f"table '{table}' does not exist")
            for k, v in records.items():
                old_val = self._data[table].get(k)
                self._data[table][k] = v
                if self._index:
                    for f in self._index.get_indexed_fields(table):
                        self._index.update_index(table, f, k, old_val, v)
                if self._wal:
                    self._wal.append(WALEntry(OP_WRITE, table, k, v))

        self._dirty.set()
        if self._metrics:
            self._metrics.increment_writes(len(records))

    def read(self, table: str, key: str) -> Tuple[Any, bool]:
        """Read a single record. Returns (value, True) or (None, False)."""
        t0 = time.monotonic()
        if self._closed.is_set():
            return None, False

        with self._data_lock.read():
            tbl = self._data.get(table)
            if tbl is None:
                return None, False
            val = tbl.get(key)
            exists = key in tbl

        if self._metrics:
            self._metrics.increment_reads()
            self._metrics.record_read_latency(time.monotonic() - t0)

        return val, exists

    def read_batch(self, table: str) -> List[Any]:
        """Return all records in a table as a list."""
        if self._closed.is_set():
            raise RuntimeError("database is closed")

        with self._data_lock.read():
            tbl = self._data.get(table)
            if tbl is None:
                raise KeyError(f"table '{table}' does not exist")
            result = list(tbl.values())

        if self._metrics:
            self._metrics.increment_reads()

        return result

    def delete(self, table: str, key: str) -> None:
        """Delete a record from a table."""
        self._send_op(_WriteOp("delete", table, key))

    # -----------------------------------------------------------------------
    # Index operations
    # -----------------------------------------------------------------------

    def create_index(self, table: str, field: str) -> None:
        """Create a hash index on *field* for O(1) lookups."""
        if not self._cfg.enable_indexes or self._index is None:
            raise RuntimeError("indexes are disabled")
        with self._data_lock.read():
            table_data = self._data.get(table)
        self._index.create_index(table, field, table_data)

    def query_by_index(self, table: str, field: str, value: Any) -> List[Any]:
        """Return all records where table.field == value (requires index)."""
        if not self._cfg.enable_indexes or self._index is None:
            raise RuntimeError("indexes are disabled")

        keys = self._index.query(table, field, value)
        with self._data_lock.read():
            tbl = self._data.get(table, {})
            return [tbl[k] for k in keys if k in tbl]

    # -----------------------------------------------------------------------
    # Transaction operations
    # -----------------------------------------------------------------------

    def begin_transaction(self) -> Transaction:
        """Begin an ACID transaction (snapshot isolation)."""
        if not self._cfg.enable_transactions or self._tx is None:
            raise RuntimeError("transactions are disabled")
        with self._data_lock.read():
            snap = self._snapshot()
        return self._tx.begin(snap)

    def commit_transaction(self, txn: Transaction) -> None:
        """Commit a transaction — merges changes into the main data store."""
        if self._tx is None:
            raise RuntimeError("transactions are disabled")

        changes = self._tx.commit(txn)
        with self._data_lock.write():
            for table, records in changes.items():
                if table not in self._data:
                    self._data[table] = {}
                for k, v in records.items():
                    if v is None:
                        old_val = self._data[table].pop(k, None)
                        if self._index:
                            for f in self._index.get_indexed_fields(table):
                                self._index.delete_from_index(table, f, k, old_val)
                        if self._wal:
                            self._wal.append(WALEntry(OP_DELETE, table, k))
                        if self._metrics:
                            self._metrics.increment_deletes()
                    else:
                        old_val = self._data[table].get(k)
                        self._data[table][k] = v
                        if self._index:
                            for f in self._index.get_indexed_fields(table):
                                self._index.update_index(table, f, k, old_val, v)
                        if self._wal:
                            self._wal.append(WALEntry(OP_WRITE, table, k, v))
                        if self._metrics:
                            self._metrics.increment_writes()
        self._dirty.set()

    # -----------------------------------------------------------------------
    # Export / Import
    # -----------------------------------------------------------------------

    def export(self, key: Key, path: str) -> None:
        """Export database as plain JSON (cross-language compatible)."""
        if self._closed.is_set():
            raise RuntimeError("database is closed")
        if not is_equal(key.bytes(), self._cfg.encryption_key.bytes()):
            raise PermissionError("unauthorized: key mismatch")
        with self._data_lock.read():
            self._storage.export_plain(self._data, path)

    def import_db(self, key: Key, path: str) -> None:
        """Import database from plain JSON."""
        if self._closed.is_set():
            raise RuntimeError("database is closed")
        if not is_equal(key.bytes(), self._cfg.encryption_key.bytes()):
            raise PermissionError("unauthorized: key mismatch")
        data = self._storage.import_plain(path)
        with self._data_lock.write():
            self._data = data
            self._rebuild_indexes_locked()
        self._dirty.set()
        self._flush_to_disk()

    def export_encrypted(self, key: Key, path: str) -> None:
        """Export database in encrypted binary format."""
        if self._closed.is_set():
            raise RuntimeError("database is closed")
        if not is_equal(key.bytes(), self._cfg.encryption_key.bytes()):
            raise PermissionError("unauthorized: key mismatch")
        with self._data_lock.read():
            self._storage.export_encrypted(self._data, path)

    def import_encrypted(self, key: Key, path: str) -> None:
        """Import database from encrypted binary format."""
        if self._closed.is_set():
            raise RuntimeError("database is closed")
        if not is_equal(key.bytes(), self._cfg.encryption_key.bytes()):
            raise PermissionError("unauthorized: key mismatch")
        data = self._storage.import_encrypted(path)
        with self._data_lock.write():
            self._data = data
            self._rebuild_indexes_locked()
        self._dirty.set()
        self._flush_to_disk()

    def change_key(self, old_key: Key, new_key: Key) -> None:
        """Rotate encryption key without downtime."""
        if not is_equal(old_key.bytes(), self._cfg.encryption_key.bytes()):
            raise PermissionError("old key does not match")
        self._cfg.encryption_key = new_key
        self._storage.update_key(new_key)
        self._dirty.set()
        self._flush_to_disk()

    # -----------------------------------------------------------------------
    # Monitoring
    # -----------------------------------------------------------------------

    def get_metrics(self) -> MetricsSnapshot:
        """Return a point-in-time metrics snapshot."""
        if self._metrics is None:
            from .modules.metrics import Snapshot

            return Snapshot()
        return self._metrics.get_snapshot()

    def get_status(self) -> Dict[str, Any]:
        """Return a status dict matching the Go GetStatus() output."""
        m = self.get_metrics()
        return {
            "uptime_seconds": time.time() - m.uptime,
            "closed": self._closed.is_set(),
            "dirty": self._dirty.is_set(),
            "total_reads": m.total_reads,
            "total_writes": m.total_writes,
            "total_deletes": m.total_deletes,
            "failed_ops": m.failed_ops,
            "avg_read_latency": m.avg_read_latency,
            "avg_write_latency": m.avg_write_latency,
            "buffered_ops": self._write_buf.qsize(),
            "last_save": self._last_save,
        }

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Close the database (30-second timeout)."""
        self.close_with_timeout(30.0)

    def close_with_timeout(self, timeout: float) -> None:
        """Gracefully shut down the database within *timeout* seconds."""
        if self._closed.is_set():
            return
        self._closed.set()
        self._stop.set()

        deadline = time.monotonic() + timeout
        for t in self._threads:
            remaining = max(0.0, deadline - time.monotonic())
            t.join(timeout=remaining)

        # Final flush
        if self._dirty.is_set():
            try:
                self._flush_to_disk()
            except Exception as exc:
                self._log.warnf("Final flush failed: %s", exc)

        if self._wal:
            try:
                self._wal.close()
            except Exception:
                pass

        self._log.info("PolarysDB closed")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # -----------------------------------------------------------------------
    # Internal — background workers
    # -----------------------------------------------------------------------

    def _start_background_workers(self) -> None:
        workers = [
            ("write-buffer", self._process_write_buffer),
            ("periodic-save", self._periodic_save),
            ("file-watcher", self._file_on_change),
        ]
        if self._wal:
            workers.append(("wal-sync", lambda: self._wal.start(self._stop)))
        if self._backup:
            workers.append(
                (
                    "backup",
                    lambda: self._backup.start(
                        self._stop, self._create_backup_snapshot
                    ),
                )
            )

        for name, fn in workers:
            t = threading.Thread(target=fn, name=f"polarysdb-{name}", daemon=True)
            t.start()
            self._threads.append(t)

    def _process_write_buffer(self) -> None:
        """Drain the write buffer in micro-batches (mirrors Go's Group Commit)."""
        BATCH_INTERVAL = 0.1  # 100 ms
        pending: List[_WriteOp] = []

        def process():
            if not pending:
                return
            with self._data_lock.write():
                for op in pending:
                    err = None
                    try:
                        if op.op_type == "create":
                            if op.table not in self._data:
                                self._data[op.table] = {}
                            if self._wal:
                                self._wal.append(WALEntry(OP_CREATE, op.table))

                        elif op.op_type == "write":
                            if op.table not in self._data:
                                err = KeyError(f"table '{op.table}' does not exist")
                            else:
                                old_val = self._data[op.table].get(op.key)
                                self._data[op.table][op.key] = op.value
                                if self._index:
                                    for f in self._index.get_indexed_fields(op.table):
                                        self._index.update_index(
                                            op.table, f, op.key, old_val, op.value
                                        )
                                if self._wal:
                                    self._wal.append(
                                        WALEntry(OP_WRITE, op.table, op.key, op.value)
                                    )
                                if self._metrics:
                                    self._metrics.increment_writes()

                        elif op.op_type == "delete":
                            if op.table not in self._data:
                                err = KeyError(f"table '{op.table}' does not exist")
                            else:
                                old_val = self._data[op.table].pop(op.key, None)
                                if self._index:
                                    for f in self._index.get_indexed_fields(op.table):
                                        self._index.delete_from_index(
                                            op.table, f, op.key, old_val
                                        )
                                if self._wal:
                                    self._wal.append(
                                        WALEntry(OP_DELETE, op.table, op.key)
                                    )
                                if self._metrics:
                                    self._metrics.increment_deletes()

                    except Exception as exc:
                        err = exc
                        if self._metrics:
                            self._metrics.increment_failed_ops()

                    op.result.put(err)

            self._dirty.set()
            pending.clear()

        next_tick = time.monotonic() + BATCH_INTERVAL
        while not self._stop.is_set():
            now = time.monotonic()
            timeout = max(0.0, next_tick - now)
            try:
                op = self._write_buf.get(timeout=timeout)
                pending.append(op)
                # Fill up to 50 ops without waiting
                while len(pending) < 50:
                    try:
                        pending.append(self._write_buf.get_nowait())
                    except queue.Empty:
                        break
            except queue.Empty:
                pass

            if time.monotonic() >= next_tick or len(pending) >= 50:
                process()
                next_tick = time.monotonic() + BATCH_INTERVAL

        # Drain remaining
        while True:
            try:
                pending.append(self._write_buf.get_nowait())
            except queue.Empty:
                break
        process()

    def _periodic_save(self) -> None:
        """Persist dirty data to disk at the configured interval."""
        while not self._stop.is_set():
            self._stop.wait(self._cfg.save_interval)
            if self._dirty.is_set():
                try:
                    self._flush_to_disk()
                except Exception as exc:
                    self._log.errorf("Periodic save failed: %s", exc)
                    if self._metrics:
                        self._metrics.increment_failed_ops()

        # Final save on shutdown
        if self._dirty.is_set():
            try:
                self._flush_to_disk()
            except Exception:
                pass

    def _file_on_change(self) -> None:
        """Reload from disk if another process modified the database file."""
        while not self._stop.is_set():
            self._stop.wait(self._cfg.watch_interval)
            if self._closed.is_set():
                break
            try:
                mtime = os.path.getmtime(self._storage.get_path())
            except OSError:
                continue

            if mtime == self._last_loaded:
                continue
            if self._dirty.is_set() or self._write_buf.qsize() > 0:
                continue

            with self._data_lock.write():
                try:
                    data, mod = self._storage.load()
                    self._data = data
                    self._rebuild_indexes_locked()
                    self._last_loaded = mod
                except Exception as exc:
                    self._log.warnf("Auto-reload error: %s", exc)

    # -----------------------------------------------------------------------
    # Internal — storage helpers
    # -----------------------------------------------------------------------

    def _flush_to_disk(self) -> None:
        t0 = time.monotonic()
        with self._data_lock.read():
            self._storage.save(self._data)
        self._dirty.clear()
        self._last_save = time.time()
        elapsed = time.monotonic() - t0
        if self._metrics:
            self._metrics.record_save_duration(elapsed)
        if self._cfg.debug:
            self._log.debugf("Flushed to disk in %.3fs", elapsed)

    def _load_with_retry(self) -> None:
        for attempt in range(self._cfg.max_retries):
            try:
                data, mtime = self._storage.load()
                with self._data_lock.write():
                    self._data = data
                    self._rebuild_indexes_locked()
                    self._last_loaded = mtime
                return
            except Exception as exc:
                self._log.warnf("Load attempt %d failed: %s", attempt + 1, exc)
                time.sleep(self._cfg.retry_delay * (attempt + 1))
        # Not fatal — start with empty state
        self._log.warn("Could not load existing data; starting fresh")

    def _recover_from_wal(self) -> int:
        if not self._wal:
            return 0
        entries = self._wal.read_all()
        if not entries:
            return 0
        self._log.infof("WAL recovery: replaying %d entries", len(entries))
        recovered = 0
        for entry in entries:
            try:
                self._apply_wal_entry(entry)
                recovered += 1
            except Exception as exc:
                self._log.warnf("Skipping bad WAL entry: %s", exc)
        self._log.info("WAL recovery complete")
        return recovered

    def _apply_wal_entry(self, entry: WALEntry) -> None:
        if entry.op_type == OP_CREATE:
            if entry.table not in self._data:
                self._data[entry.table] = {}
        elif entry.op_type == OP_WRITE:
            if entry.table not in self._data:
                raise KeyError(f"table '{entry.table}' does not exist")
            self._data[entry.table][entry.key] = entry.value
        elif entry.op_type == OP_DELETE:
            if entry.table in self._data:
                self._data[entry.table].pop(entry.key, None)

    def _snapshot(self) -> Dict[str, Dict[str, Any]]:
        return copy.deepcopy(self._data)

    def _rebuild_indexes_locked(self) -> None:
        if not self._index:
            return
        for table in self._data:
            self._index.rebuild_table(table, self._data.get(table))

    def _create_backup_snapshot(self) -> bytes:
        with self._data_lock.read():
            return self._storage.serialize(self._data)

    def _send_op(self, op: _WriteOp) -> None:
        if self._closed.is_set():
            raise RuntimeError("database is closed")
        try:
            self._write_buf.put(op, timeout=_WRITE_TIMEOUT)
        except queue.Full:
            raise TimeoutError("write buffer full — operation timed out")
        err = op.result.get(timeout=_WRITE_TIMEOUT + 1)
        if err is not None:
            raise err


# ---------------------------------------------------------------------------
# Fair read-write lock (threading.RWLock not in stdlib)
# ---------------------------------------------------------------------------


class _FairRWLock:
    """Simple readers-writer lock using standard threading primitives."""

    def __init__(self):
        self._lock = threading.Lock()
        self._readers = 0
        self._write_lock = threading.Lock()

    class _ReadCtx:
        def __init__(self, rwl):
            self._rwl = rwl

        def __enter__(self):
            with self._rwl._lock:
                self._rwl._readers += 1
                if self._rwl._readers == 1:
                    self._rwl._write_lock.acquire()

        def __exit__(self, *_):
            with self._rwl._lock:
                self._rwl._readers -= 1
                if self._rwl._readers == 0:
                    self._rwl._write_lock.release()

    class _WriteCtx:
        def __init__(self, rwl):
            self._rwl = rwl

        def __enter__(self):
            self._rwl._write_lock.acquire()

        def __exit__(self, *_):
            self._rwl._write_lock.release()

    def read(self):
        return self._ReadCtx(self)

    def write(self):
        return self._WriteCtx(self)
