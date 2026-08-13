"""
polarysdb.modules.index
In-memory hash index manager — mirrors the Go index.Manager.

Each index maps:  (table, field) → {value → [list of record keys]}
"""
from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any

from .logger import Logger


class Manager:
    """
    Manages hash-based in-memory indexes across tables and fields.
    Provides O(1) lookups by indexed field value.
    """

    def __init__(self, logger: Logger | None = None):
        self._logger = logger
        # Structure: _indexes[table][field][value] = {key, ...}
        self._indexes: dict[str, dict[str, dict[Any, set]]] = {}
        self._lock = threading.RWLock() if hasattr(threading, "RWLock") else None
        self._rlock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_index(
        self,
        table: str,
        field: str,
        table_data: dict[str, Any] | None = None,
    ) -> None:
        """
        Create an index on *field* for *table*.
        If table_data is provided, the index is populated immediately.
        """
        with self._rlock:
            if table not in self._indexes:
                self._indexes[table] = {}
            if field in self._indexes[table]:
                return  # already exists

            idx: dict[Any, set] = defaultdict(set)
            if table_data:
                for rec_key, record in table_data.items():
                    val = self._extract(record, field)
                    if val is not None:
                        idx[val].add(rec_key)

            self._indexes[table][field] = idx

            if self._logger:
                self._logger.infof("Index created: %s.%s", table, field)

    def get_indexed_fields(self, table: str) -> list[str]:
        """Return list of indexed fields for a table."""
        with self._rlock:
            return list(self._indexes.get(table, {}).keys())

    def query(self, table: str, field: str, value: Any) -> list[str]:
        """
        Return record keys where table.field == value.
        Raises KeyError if the index does not exist.
        """
        with self._rlock:
            tbl = self._indexes.get(table)
            if tbl is None:
                raise KeyError(f"no indexes on table '{table}'")
            idx = tbl.get(field)
            if idx is None:
                raise KeyError(f"no index on field '{field}' in table '{table}'")
            return list(idx.get(value, set()))

    def update_index(
        self,
        table: str,
        field: str,
        rec_key: str,
        old_record: Any,
        new_record: Any,
    ) -> None:
        """
        Update the index when a record is overwritten.
        Removes the old value bucket entry and adds the new one.
        """
        with self._rlock:
            idx = self._indexes.get(table, {}).get(field)
            if idx is None:
                return

            old_val = self._extract(old_record, field)
            new_val = self._extract(new_record, field)

            if old_val is not None and old_val in idx:
                idx[old_val].discard(rec_key)
                if not idx[old_val]:
                    del idx[old_val]

            if new_val is not None:
                idx[new_val].add(rec_key)

    def delete_from_index(
        self,
        table: str,
        field: str,
        rec_key: str,
        old_record: Any,
    ) -> None:
        """Remove a record from all index buckets."""
        with self._rlock:
            idx = self._indexes.get(table, {}).get(field)
            if idx is None:
                return

            old_val = self._extract(old_record, field)
            if old_val is not None and old_val in idx:
                idx[old_val].discard(rec_key)
                if not idx[old_val]:
                    del idx[old_val]

    def drop_table(self, table: str) -> None:
        """Remove all indexes for a table."""
        with self._rlock:
            self._indexes.pop(table, None)

    def rebuild_table(
        self,
        table: str,
        table_data: dict[str, Any] | None = None,
    ) -> None:
        """
        Rebuild all existing indexes for *table* from *table_data*.
        No-op if the table has no indexes.
        """
        with self._rlock:
            fields = list(self._indexes.get(table, {}).keys())
            if not fields:
                return

            rebuilt: dict[str, dict[Any, set]] = {}
            for field in fields:
                idx: dict[Any, set] = defaultdict(set)
                if table_data:
                    for rec_key, record in table_data.items():
                        val = self._extract(record, field)
                        if val is not None:
                            idx[val].add(rec_key)
                rebuilt[field] = idx

            self._indexes[table] = rebuilt

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _extract(record: Any, field: str) -> Any | None:
        """Extract field value from a record (dict or object)."""
        if isinstance(record, dict):
            return record.get(field)
        return getattr(record, field, None)
