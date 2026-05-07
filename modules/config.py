"""
polarysdb.modules.config
Path helpers mirroring the Go modules/config package.

Go reference:
  modules/config/config.go

Important behavior:
  - If *dir* is relative, state is stored under:  ~/dir/state/state.rdb
  - If *dir* is absolute, state is stored under:  dir/state/state.rdb
"""

from __future__ import annotations

import os


def get_home_subdir(subdir: str, dir: str) -> str:
    """
    Create/return a subdirectory under the user's home directory.

    Mirrors Go:
      filepath.Join(homeDir, dir, subdir)

    Note: if *dir* is absolute, os.path.join(home, dir, subdir) returns dir/subdir,
    which matches Go filepath.Join behavior.
    """
    home_dir = os.path.expanduser("~")
    sub_dir_path = os.path.join(home_dir, dir, subdir)
    os.makedirs(sub_dir_path, mode=0o777, exist_ok=True)
    return sub_dir_path


def get_state_db_path(dir: str) -> str:
    """Return the full path to the state database file: .../state/state.rdb"""
    return os.path.join(get_home_subdir("state", dir), "state.rdb")

