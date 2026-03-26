import importlib

import pytest


def test_sector_dal_module_is_removed():
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("pyqmt.data.dal.sector_dal")