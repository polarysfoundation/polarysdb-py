"""
polarysdb.modules.backup
Automatic backup manager with rotation — mirrors the Go backup.Manager.
"""

import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from .logger import Logger


@dataclass
class Config:
    backup_dir: str
    interval: float = 3600.0     # seconds between automatic backups
    keep_count: int = 10          # number of backups to retain


SnapshotFn = Callable[[], bytes]


class Manager:
    """
    Runs automatic periodic backups and prunes old ones.
    Mirrors the Go backup.Manager interface.
    """

    def __init__(self, cfg: Config, logger: Optional[Logger] = None):
        self.cfg = cfg
        self._logger = logger
        Path(cfg.backup_dir).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self, stop_event: threading.Event, snapshot_fn: SnapshotFn) -> None:
        """
        Background backup loop — call from a dedicated daemon thread.
        Stops when stop_event is set.
        """
        while not stop_event.is_set():
            stop_event.wait(self.cfg.interval)
            if stop_event.is_set():
                break
            try:
                self._do_backup(snapshot_fn)
            except Exception as exc:
                if self._logger:
                    self._logger.warnf("Backup error: %s", exc)

    def backup_now(self, snapshot_fn: SnapshotFn) -> str:
        """Trigger an immediate backup. Returns the backup file path."""
        return self._do_backup(snapshot_fn)

    def list_backups(self):
        """Return sorted list of backup file paths (oldest first)."""
        files = []
        for f in Path(self.cfg.backup_dir).glob("polarysdb_*.bak"):
            files.append((f.stat().st_mtime, str(f)))
        files.sort()
        return [p for _, p in files]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _do_backup(self, snapshot_fn: SnapshotFn) -> str:
        ts = time.strftime("%Y%m%d_%H%M%S")
        name = f"polarysdb_{ts}.bak"
        path = os.path.join(self.cfg.backup_dir, name)

        data = snapshot_fn()
        with open(path, "wb") as f:
            f.write(data)

        if self._logger:
            self._logger.infof("Backup created: %s (%d bytes)", name, len(data))

        self._prune()
        return path

    def _prune(self) -> None:
        """Remove oldest backups exceeding keep_count."""
        backups = self.list_backups()
        to_remove = backups[: max(0, len(backups) - self.cfg.keep_count)]
        for path in to_remove:
            try:
                os.unlink(path)
                if self._logger:
                    self._logger.debugf("Pruned old backup: %s", path)
            except OSError:
                pass
