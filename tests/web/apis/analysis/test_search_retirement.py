import pytest

from pyqmt.web.apis.analysis.search import search_stocks


@pytest.mark.asyncio
async def test_index_search_returns_gone():
    response = await search_stocks(None, q="000300", type="index")

    assert response.status_code == 410
    assert b"index search has been retired from the subject app" in response.body