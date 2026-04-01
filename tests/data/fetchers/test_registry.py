from quantide.data.fetchers.registry import DataFetcherRegistry, register_builtin_fetchers
from quantide.data.fetchers.tushare import TushareDataFetcher


class DummyFetcher:
    pass


def test_registry_registers_tushare_as_default():
    registry = register_builtin_fetchers(DataFetcherRegistry())

    assert registry.default_name == "tushare"
    assert isinstance(registry.get(), TushareDataFetcher)
    assert registry.list_names() == ["tushare"]


def test_registry_can_override_default_fetcher():
    registry = DataFetcherRegistry()
    registry.register("dummy", DummyFetcher(), make_default=True)

    assert registry.default_name == "dummy"
    assert isinstance(registry.get(), DummyFetcher)