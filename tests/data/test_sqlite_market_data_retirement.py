import sqlite3

from pyqmt.data.sqlite import db


def test_sqlite_init_drops_legacy_market_tables(tmp_path):
    db_path = tmp_path / "legacy_market.db"

    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE sector_bars (sector_id TEXT, dt TEXT)")
    conn.execute("CREATE TABLE index_bars (symbol TEXT, dt TEXT)")
    conn.execute("CREATE TABLE sectors (id TEXT, trade_date TEXT)")
    conn.execute("CREATE TABLE sector_constituents (sector_id TEXT, trade_date TEXT, symbol TEXT)")
    conn.execute("CREATE TABLE indices (symbol TEXT)")
    conn.execute("CREATE TABLE keep_me (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    db.init(db_path)

    tables = set(db.table_names())
    assert "sector_bars" not in tables
    assert "index_bars" not in tables
    assert "sectors" not in tables
    assert "sector_constituents" not in tables
    assert "indices" not in tables
    assert "keep_me" in tables
    assert "orders" in tables


def test_sqlite_never_creates_market_bar_tables(tmp_path):
    db_path = tmp_path / "fresh.db"

    db.init(db_path)

    tables = set(db.table_names())
    assert "sector_bars" not in tables
    assert "index_bars" not in tables
    assert "sectors" not in tables
    assert "sector_constituents" not in tables
    assert "indices" not in tables