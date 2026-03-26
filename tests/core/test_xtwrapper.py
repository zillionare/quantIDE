import pytest

from pyqmt.core import xtwrapper


@pytest.mark.parametrize(
    ("func_name", "args"),
    [
        ("require_xt", ()),
        ("subcribe_live", ()),
        ("cache_bars", (None,)),
        ("get_bars", ([], None, None, None)),
        ("get_stock_list", ()),
        ("get_sectors", ()),
        ("get_calendar", ()),
        ("get_security_info", ("000001.SZ",)),
        ("get_factor_ratio", ("000001.SZ", None, None)),
    ],
)
def test_xtwrapper_functions_are_removed(func_name, args):
    func = getattr(xtwrapper, func_name)

    with pytest.raises(RuntimeError, match="主体移除"):
        func(*args)