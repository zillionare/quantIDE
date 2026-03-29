import pytest

from quantide.web.apis.analysis.kline import get_sector_kline


@pytest.mark.asyncio
async def test_sector_kline_returns_gone():
    response = await get_sector_kline(None, "sw1.power")

    assert response.status_code == 410
    assert b"sector kline has been retired from the subject app" in response.body