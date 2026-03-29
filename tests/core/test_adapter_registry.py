from quantide.core.runtime import AdapterRegistry


def test_adapter_registry_register_and_resolve():
    registry = AdapterRegistry()
    adapter = object()

    registry.register("broker", "legacy", adapter)

    assert registry.resolve("broker", "legacy") is adapter


def test_adapter_registry_list_specs():
    registry = AdapterRegistry()
    adapter_a = object()
    adapter_b = object()

    registry.register("broker", "a", adapter_a)
    registry.register("market_data", "b", adapter_b)

    specs = registry.list_specs()
    keys = {(item.capability, item.name) for item in specs}

    assert ("broker", "a") in keys
    assert ("market_data", "b") in keys
