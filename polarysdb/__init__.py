"""
polarysdb — Python edition
High-performance embedded database with AES-256 encryption, WAL, and ACID transactions.
Python port of github.com/polarysfoundation/polarysdb — file-format compatible.

Quick start:
    from polarysdb import init, Key

    key = Key("my-secret-encryption-key-32b")
    db  = init(key, "./data", debug=False)

    db.create("users")
    db.write("users", "alice", {"name": "Alice", "age": 30})

    value, ok = db.read("users", "alice")
    db.close()
"""

from .database import Config, Database, default_config
from .modules.common import Key, is_equal
from .modules.config import get_state_db_path
from .modules.metrics import Snapshot as MetricsSnapshot
from .modules.tx import Transaction

__version__ = "1.1.0"
__author__ = "Polarys Foundation (Python port)"
__all__ = [
    "Config",
    "Database",
    "Key",
    "MetricsSnapshot",
    "Transaction",
    "default_config",
    "init",
    "init_with_config",
    "is_equal",
]


def init(key: Key, dir_path: str, debug: bool = False) -> Database:
    """
    Initialize the database with minimal configuration.
    Mirrors the Go polarysdb.Init() function exactly.

    Args:
        key:      32-byte encryption key (use Key("passphrase") or Key.generate())
        dir_path: directory where data files are stored
        debug:    enable verbose debug logging

    Returns:
        A ready-to-use Database instance.
    """
    cfg = default_config()
    cfg.dir_path = dir_path
    cfg.encryption_key = key
    cfg.debug = debug
    return init_with_config(cfg)


def init_with_config(cfg: Config) -> Database:
    """
    Initialize the database with full configuration.
    Mirrors the Go polarysdb.InitWithConfig() function exactly.
    """
    _validate_config(cfg)
    _setup_directories(cfg)
    return Database(cfg)


def _validate_config(cfg: Config) -> None:
    if not cfg.dir_path:
        raise ValueError("dir_path cannot be empty")
    if cfg.save_interval < 0.1:
        raise ValueError("save_interval too small (< 0.1 s)")
    if cfg.buffer_size < 10:
        raise ValueError("buffer_size too small (< 10)")


def _setup_directories(cfg: Config) -> None:
    import os

    dirs = [cfg.dir_path]
    if cfg.enable_backup:
        dirs.append(cfg.backup_dir)
    # Match Go layout: state DB lives under .../state/state.rdb
    dirs.append(os.path.dirname(get_state_db_path(cfg.dir_path)))
    for d in dirs:
        os.makedirs(d, mode=0o700, exist_ok=True)
