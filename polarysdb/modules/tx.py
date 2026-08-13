"""
polarysdb.modules.tx
ACID transaction manager with snapshot isolation —
mirrors the Go tx.Manager and tx.Transaction types.
"""
from __future__ import annotations

import copy
import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .logger import Logger


class TxState(Enum):
    ACTIVE    = "active"
    COMMITTED = "committed"
    ABORTED   = "aborted"


@dataclass
class _Op:
    op: str   # "write" | "delete"
    table: str
    key: str
    value: Any


class Transaction:
    """
    A single ACID transaction.
    Holds a copy-on-write snapshot of the database at the moment Begin() was called.
    Changes are buffered locally until Commit() or Rollback().
    """

    def __init__(self, tx_id: str, snapshot: dict[str, dict[str, Any]]):
        self._id = tx_id
        self._snapshot = snapshot           # read baseline (immutable view)
        self._ops: list[_Op] = []          # pending operations
        self._state = TxState.ACTIVE
        self._started_at = time.time()
        self._lock = threading.Lock()

        # Local working copy (starts as snapshot, receives pending writes)
        self._working: dict[str, dict[str, Any]] = copy.deepcopy(snapshot)

    # ------------------------------------------------------------------
    # Public API (mirrors Go tx.Transaction)
    # ------------------------------------------------------------------

    @property
    def id(self) -> str:
        return self._id

    def write(self, table: str, key: str, value: Any) -> None:
        """Buffer a write operation in this transaction."""
        with self._lock:
            self._ensure_active()
            if table not in self._working:
                raise KeyError(f"table '{table}' does not exist")
            self._working[table][key] = value
            self._ops.append(_Op("write", table, key, copy.deepcopy(value)))

    def delete(self, table: str, key: str) -> None:
        """Buffer a delete operation in this transaction."""
        with self._lock:
            self._ensure_active()
            if table not in self._working:
                raise KeyError(f"table '{table}' does not exist")
            self._working[table].pop(key, None)
            self._ops.append(_Op("delete", table, key, None))

    def read(self, table: str, key: str) -> tuple[Any, bool]:
        """Read from the transaction's working copy."""
        with self._lock:
            self._ensure_active()
            tbl = self._working.get(table)
            if tbl is None:
                return None, False
            val = tbl.get(key)
            return val, val is not None

    def rollback(self) -> None:
        """Discard all buffered changes."""
        with self._lock:
            self._state = TxState.ABORTED

    # ------------------------------------------------------------------
    # Internal — called by Manager
    # ------------------------------------------------------------------

    def _get_changes(self) -> dict[str, dict[str, Any]]:
        """
        Return the delta dict consumed by Manager.commit().
        Value is None for deletes.
        """
        changes: dict[str, dict[str, Any]] = {}
        for op in self._ops:
            if op.table not in changes:
                changes[op.table] = {}
            if op.op == "write":
                changes[op.table][op.key] = op.value
            else:  # delete
                changes[op.table][op.key] = None
        return changes

    def _mark_committed(self) -> None:
        self._state = TxState.COMMITTED

    def _ensure_active(self) -> None:
        if self._state != TxState.ACTIVE:
            raise RuntimeError(
                f"transaction {self._id} is not active (state={self._state.value})"
            )


class Manager:
    """
    Transaction manager — mirrors the Go tx.Manager.
    Tracks active transactions and applies committed changes.
    """

    def __init__(self, logger: Logger | None = None):
        self._logger = logger
        self._active: dict[str, Transaction] = {}
        self._lock = threading.Lock()

    def begin(self, snapshot: dict[str, dict[str, Any]]) -> Transaction:
        """Start a new transaction with a copy of the current database state."""
        tx_id = str(uuid.uuid4())
        txn = Transaction(tx_id, snapshot)
        with self._lock:
            self._active[tx_id] = txn
        if self._logger:
            self._logger.debugf("Transaction %s started", tx_id)
        return txn

    def commit(self, txn: Transaction) -> dict[str, dict[str, Any]]:
        """
        Commit a transaction.
        Returns the change-set dict to be merged into the main data store.
        Values of None indicate a delete.
        """
        with self._lock:
            if txn.id not in self._active:
                raise RuntimeError(f"transaction {txn.id} is not registered")

        changes = txn._get_changes()
        txn._mark_committed()

        with self._lock:
            self._active.pop(txn.id, None)

        if self._logger:
            self._logger.debugf("Transaction %s committed (%d ops)", txn.id, len(changes))

        return changes

    def active_count(self) -> int:
        with self._lock:
            return len(self._active)
