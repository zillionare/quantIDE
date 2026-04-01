from __future__ import annotations

import os
import sys
from pathlib import Path


DEFAULT_DATA_HOME = "~/.quantide"
_APP_CONFIG_DIR_OVERRIDE: Path | None = None


def normalize_data_home(home: str | Path | None = None) -> str:
    """Normalize the configured market-data home path.

    Blank values fall back to the default per-user data directory and any
    leading `~` is expanded for persistence.
    """
    text = str(home or "").strip() or DEFAULT_DATA_HOME
    return str(Path(text).expanduser())


def set_app_config_dir_override(path: str | Path | None) -> Path | None:
    """Override the app config directory for the current process.

    This is primarily used by tests that need an isolated sqlite database,
    pid file, and other runtime state under a temporary directory.
    """

    global _APP_CONFIG_DIR_OVERRIDE
    previous = _APP_CONFIG_DIR_OVERRIDE
    if path is None or not str(path).strip():
        _APP_CONFIG_DIR_OVERRIDE = None
    else:
        _APP_CONFIG_DIR_OVERRIDE = Path(path).expanduser()
    return previous


def clear_app_config_dir_override() -> None:
    """Clear the process-level app config directory override."""

    global _APP_CONFIG_DIR_OVERRIDE
    _APP_CONFIG_DIR_OVERRIDE = None


def get_app_config_dir() -> Path:
    """Return the per-user configuration directory for Quantide."""
    if _APP_CONFIG_DIR_OVERRIDE is not None:
        return _APP_CONFIG_DIR_OVERRIDE

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
    "DEFAULT_DATA_HOME",
    "clear_app_config_dir_override",
    "ensure_app_config_dir",
    "get_app_config_dir",
    "get_app_db_path",
    "get_pid_file_path",
    "get_strategy_runtime_state_path",
    "normalize_data_home",
    "set_app_config_dir_override",
]
