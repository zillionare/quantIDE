import pytest

import pyqmt.data.services as services
from pyqmt.data.fetchers.xtdata_sectors import fetch_sector_list
from pyqmt.data.services.scheduler import DataSyncScheduler


def test_data_services_only_exports_published_sync_surface():
    assert services.__all__ == ["StockSyncService"]
    assert not hasattr(services, "IndexSyncService")
    assert not hasattr(services, "SectorSyncService")


def test_xtdata_fetcher_is_removed():
    with pytest.raises(RuntimeError, match="板块/指数"):
        fetch_sector_list()


def test_scheduler_is_removed():
    with pytest.raises(RuntimeError, match="数据同步调度"):
        DataSyncScheduler()