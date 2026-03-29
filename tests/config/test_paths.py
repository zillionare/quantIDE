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
