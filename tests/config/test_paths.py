from pathlib import Path

import quantide.config.paths as paths_module


def test_get_app_config_dir_uses_xdg_home(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(paths_module.sys, "platform", "linux")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg-config"))

    assert paths_module.get_app_config_dir() == tmp_path / "xdg-config" / "quantide"


def test_get_app_config_dir_uses_appdata_on_windows(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(paths_module.sys, "platform", "win32")
    monkeypatch.setenv("APPDATA", str(tmp_path / "AppData" / "Roaming"))

    assert paths_module.get_app_config_dir() == tmp_path / "AppData" / "Roaming" / "quantide"


def test_get_app_config_dir_prefers_explicit_override(tmp_path: Path):
    paths_module.set_app_config_dir_override(tmp_path / "custom-config")

    try:
        assert paths_module.get_app_config_dir() == tmp_path / "custom-config"
    finally:
        paths_module.clear_app_config_dir_override()


def test_normalize_data_home_expands_user_and_defaults_blank():
    assert paths_module.normalize_data_home("") == str(Path("~/.quantide").expanduser())
    assert paths_module.normalize_data_home("~/market-data") == str(Path("~/market-data").expanduser())
