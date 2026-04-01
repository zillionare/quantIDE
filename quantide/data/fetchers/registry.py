"""标准数据源适配器注册表."""

from __future__ import annotations

from quantide.core.ports import DataFetcherPort


class DataFetcherRegistry:
    """按名称注册和解析标准数据源适配器."""

    def __init__(self):
        self._fetchers: dict[str, DataFetcherPort] = {}
        self._default_name: str | None = None

    @property
    def default_name(self) -> str | None:
        return self._default_name

    def has(self, name: str) -> bool:
        return name in self._fetchers

    def register(
        self,
        name: str,
        fetcher: DataFetcherPort,
        make_default: bool = False,
    ) -> None:
        normalized_name = str(name or "").strip()
        if not normalized_name:
            raise ValueError("fetcher name is required")
        self._fetchers[normalized_name] = fetcher
        if make_default or self._default_name is None:
            self._default_name = normalized_name

    def get(self, name: str | None = None) -> DataFetcherPort:
        target = str(name or self._default_name or "").strip()
        if not target:
            raise KeyError("default data fetcher not configured")
        if target not in self._fetchers:
            raise KeyError(f"data fetcher not found: {target}")
        return self._fetchers[target]

    def list_names(self) -> list[str]:
        return sorted(self._fetchers)


fetcher_registry = DataFetcherRegistry()


def register_builtin_fetchers(
    registry: DataFetcherRegistry | None = None,
) -> DataFetcherRegistry:
    registry = registry or fetcher_registry
    if not registry.has("tushare"):
        from quantide.data.fetchers.tushare import TushareDataFetcher

        registry.register(
            "tushare",
            TushareDataFetcher(),
            make_default=registry.default_name is None,
        )
    return registry


def get_data_fetcher(name: str | None = None) -> DataFetcherPort:
    from quantide.config.runtime import get_runtime_data_source

    registry = register_builtin_fetchers()
    resolved_name = str(name or get_runtime_data_source() or registry.default_name or "").strip().lower()
    return registry.get(resolved_name)


__all__ = [
    "DataFetcherRegistry",
    "fetcher_registry",
    "get_data_fetcher",
    "register_builtin_fetchers",
]