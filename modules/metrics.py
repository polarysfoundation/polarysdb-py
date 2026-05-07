"""
polarysdb.modules.metrics
Real-time performance metrics — mirrors the Go metrics.Collector.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Snapshot:
    total_reads:       int   = 0
    total_writes:      int   = 0
    total_deletes:     int   = 0
    failed_ops:        int   = 0
    avg_read_latency:  float = 0.0   # seconds
    avg_write_latency: float = 0.0   # seconds
    uptime:            float = field(default_factory=time.time)
    last_save_duration: float = 0.0


class Collector:
    """
    Thread-safe metrics collector mirroring the Go metrics.Collector.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._reads     = 0
        self._writes    = 0
        self._deletes   = 0
        self._failed    = 0
        self._read_lat_sum  = 0.0
        self._write_lat_sum = 0.0
        self._save_dur  = 0.0
        self._start = time.time()

    # ------------------------------------------------------------------
    # Increment counters
    # ------------------------------------------------------------------

    def increment_reads(self, n: int = 1) -> None:
        with self._lock:
            self._reads += n

    def increment_writes(self, n: int = 1) -> None:
        with self._lock:
            self._writes += n

    def increment_deletes(self, n: int = 1) -> None:
        with self._lock:
            self._deletes += n

    def increment_failed_ops(self, n: int = 1) -> None:
        with self._lock:
            self._failed += n

    # ------------------------------------------------------------------
    # Record latencies
    # ------------------------------------------------------------------

    def record_read_latency(self, seconds: float) -> None:
        with self._lock:
            self._read_lat_sum += seconds

    def record_write_latency(self, seconds: float) -> None:
        with self._lock:
            self._write_lat_sum += seconds

    def record_save_duration(self, seconds: float) -> None:
        with self._lock:
            self._save_dur = seconds

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    def get_snapshot(self) -> Snapshot:
        with self._lock:
            avg_read  = (self._read_lat_sum  / self._reads)  if self._reads  else 0.0
            avg_write = (self._write_lat_sum / self._writes) if self._writes else 0.0
            return Snapshot(
                total_reads=self._reads,
                total_writes=self._writes,
                total_deletes=self._deletes,
                failed_ops=self._failed,
                avg_read_latency=avg_read,
                avg_write_latency=avg_write,
                uptime=self._start,
                last_save_duration=self._save_dur,
            )
