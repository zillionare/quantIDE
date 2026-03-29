import importlib

import pytest

import quantide.data.services as services
from quantide.data.services.scheduler import DataSyncScheduler


def test_data_services_only_exports_published_sync_surface():
    assert services.__all__ == ["StockSyncService"]
    assert not hasattr(services, "IndexSyncService")
    assert not hasattr(services, "SectorSyncService")


def test_xtdata_fetcher_module_is_removed():
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("quantide.data.fetchers.xtdata_sectors")


def test_scheduler_is_removed():
    with pytest.raises(RuntimeError, match="数据同步调度"):
        DataSyncScheduler()