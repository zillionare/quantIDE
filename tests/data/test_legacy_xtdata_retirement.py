import pytest

import pyqmt.data.services as services
from pyqmt.core.legacy_qmt import LEGACY_LOCAL_QMT_ENV
from pyqmt.data.fetchers.xtdata_sectors import fetch_sector_list
from pyqmt.data.services.scheduler import DataSyncScheduler


def test_data_services_only_exports_published_sync_surface():
    assert services.__all__ == ["StockSyncService"]
    assert not hasattr(services, "IndexSyncService")
    assert not hasattr(services, "SectorSyncService")


def test_xtdata_fetcher_requires_explicit_legacy_opt_in(monkeypatch):
    monkeypatch.delenv(LEGACY_LOCAL_QMT_ENV, raising=False)

    with pytest.raises(RuntimeError, match="xtdata"):
        fetch_sector_list()


def test_scheduler_requires_explicit_legacy_opt_in(monkeypatch):
    monkeypatch.delenv(LEGACY_LOCAL_QMT_ENV, raising=False)

    scheduler = DataSyncScheduler()
    scheduler._jobs_started = False

    with pytest.raises(RuntimeError, match="数据同步调度"):
        scheduler.start()