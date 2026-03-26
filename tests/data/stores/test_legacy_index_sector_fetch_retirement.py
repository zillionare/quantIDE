import pytest

from pyqmt.data.models.calendar import Calendar
from pyqmt.data.stores.index_bars import IndexBarsStore
from pyqmt.data.stores.sector_bars import SectorBarsStore


@pytest.fixture
def baseline_calendar(asset_dir):
    calendar = Calendar()
    calendar.load(asset_dir / "baseline_calendar.parquet")
    return calendar


def test_index_store_fetch_is_removed(tmp_path, baseline_calendar):
    store = IndexBarsStore(tmp_path / "index_bars", baseline_calendar)

    with pytest.raises(RuntimeError, match="指数抓取功能已从 pyqmt 主体移除"):
        store.fetch("000300.SH", baseline_calendar.epoch, baseline_calendar.epoch)


def test_sector_store_fetch_is_removed(tmp_path, baseline_calendar):
    store = SectorBarsStore(tmp_path / "sector_bars", baseline_calendar)

    with pytest.raises(RuntimeError, match="板块抓取功能已从 pyqmt 主体移除"):
        store.fetch("沪深指数", baseline_calendar.epoch, baseline_calendar.epoch)