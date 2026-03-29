import pytest

from quantide.data.models.calendar import Calendar
from quantide.data.stores.index_bars import IndexBarsStore


@pytest.fixture
def baseline_calendar(asset_dir):
    calendar = Calendar()
    calendar.load(asset_dir / "baseline_calendar.parquet")
    return calendar


def test_index_store_fetch_is_removed(tmp_path, baseline_calendar):
    store = IndexBarsStore(tmp_path / "index_bars", baseline_calendar)

    with pytest.raises(RuntimeError, match="指数抓取功能已从 quantide 主体移除"):
        store.fetch("000300.SH", baseline_calendar.epoch, baseline_calendar.epoch)