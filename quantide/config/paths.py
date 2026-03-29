from __future__ import annotations

import os
import sys
from pathlib import Path


def get_app_config_dir() -> Path:
    """Return the per-user configuration directory for Quantide."""
    if sys.platform.startswith("win"):
        base = os.environ.get("APPDATA")
        if base:
            return Path(base).expanduser() / "quantide"
        return Path.home() / "AppData" / "Roaming" / "quantide"

    base = os.environ.get("XDG_CONFIG_HOME")
    if base:
        return Path(base).expanduser() / "quantide"
    return Path.home() / ".config" / "quantide"


def ensure_app_config_dir() -> Path:
    """Create the Quantide configuration directory if needed."""
    path = get_app_config_dir()
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_app_db_path() -> Path:
    """Return the sqlite database path stored in the config directory."""
    return ensure_app_config_dir() / "quantide.db"


def get_pid_file_path() -> Path:
    """Return the single-instance PID file path."""
    return ensure_app_config_dir() / ".quantide.pid"


def get_strategy_runtime_state_path() -> Path:
    """Return the persisted strategy runtime state file path."""
    return ensure_app_config_dir() / "strategy_runtimes.json"


__all__ = [
    "ensure_app_config_dir",
    "get_app_config_dir",
    "get_app_db_path",
    "get_pid_file_path",
    "get_strategy_runtime_state_path",
]
