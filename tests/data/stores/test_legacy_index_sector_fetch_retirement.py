from pathlib import Path

import pytest

from pyqmt.core.legacy_qmt import LEGACY_LOCAL_QMT_ENV
from pyqmt.data.models.calendar import Calendar
from pyqmt.data.stores.index_bars import IndexBarsStore
from pyqmt.data.stores.sector_bars import SectorBarsStore


@pytest.fixture
def baseline_calendar(asset_dir):
    calendar = Calendar()
    calendar.load(asset_dir / "baseline_calendar.parquet")
    return calendar


def test_index_store_fetch_requires_explicit_legacy_opt_in(
    monkeypatch, tmp_path, baseline_calendar
):
    monkeypatch.delenv(LEGACY_LOCAL_QMT_ENV, raising=False)
    store = IndexBarsStore(tmp_path / "index_bars", baseline_calendar)

    with pytest.raises(RuntimeError, match="指数 xtdata 抓取"):
        store.fetch("000300.SH", baseline_calendar.epoch, baseline_calendar.epoch)


def test_sector_store_fetch_requires_explicit_legacy_opt_in(
    monkeypatch, tmp_path, baseline_calendar
):
    monkeypatch.delenv(LEGACY_LOCAL_QMT_ENV, raising=False)
    store = SectorBarsStore(tmp_path / "sector_bars", baseline_calendar)

    with pytest.raises(RuntimeError, match="板块 xtdata 抓取"):
        store.fetch("沪深指数", baseline_calendar.epoch, baseline_calendar.epoch)