from pathlib import Path

import quantide.data as data_module


def test_init_data_uses_fixed_config_db_by_default(monkeypatch, tmp_path: Path):
    data_home = tmp_path / "market-home"
    config_db_path = tmp_path / "config" / "quantide.db"
    captured: dict[str, Path | tuple[str, str]] = {}

    monkeypatch.setattr(data_module, "get_app_db_path", lambda: config_db_path)
    monkeypatch.setattr(data_module.calendar, "load", lambda path: captured.setdefault("calendar", Path(path)))
    monkeypatch.setattr(data_module.stock_list, "load", lambda path: captured.setdefault("stocks", Path(path)))
    monkeypatch.setattr(
        data_module.daily_bars,
        "connect",
        lambda store_path, calendar_path: captured.setdefault(
            "daily",
            (str(store_path), str(calendar_path)),
        ),
    )
    monkeypatch.setattr(
        data_module.index_bars,
        "connect",
        lambda store_path, calendar: captured.setdefault("index", Path(store_path)),
    )
    monkeypatch.setattr(data_module.db, "init", lambda path: captured.setdefault("db", Path(path)))

    data_module.init_data(data_home)

    assert captured["calendar"] == data_home / "data" / "calendar.parquet"
    assert captured["stocks"] == data_home / "data" / "stock_list.parquet"
    assert captured["db"] == config_db_path
    assert (data_home / "data" / "bars" / "daily").exists()
    assert (data_home / "data" / "bars" / "index").exists()


def test_init_data_allows_explicit_db_override(monkeypatch, tmp_path: Path):
    data_home = tmp_path / "market-home"
    explicit_db_path = tmp_path / "custom" / "override.db"
    captured: dict[str, Path] = {}

    monkeypatch.setattr(data_module.calendar, "load", lambda path: None)
    monkeypatch.setattr(data_module.stock_list, "load", lambda path: None)
    monkeypatch.setattr(data_module.daily_bars, "connect", lambda store_path, calendar_path: None)
    monkeypatch.setattr(data_module.index_bars, "connect", lambda store_path, calendar: None)
    monkeypatch.setattr(data_module.db, "init", lambda path: captured.setdefault("db", Path(path)))

    data_module.init_data(data_home, db_path=explicit_db_path)

    assert captured["db"] == explicit_db_path
