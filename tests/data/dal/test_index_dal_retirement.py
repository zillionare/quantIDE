import importlib

import pytest


def test_index_dal_module_is_removed():
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("quantide.data.dal.index_dal")