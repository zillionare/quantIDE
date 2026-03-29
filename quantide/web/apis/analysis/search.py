"""搜索 API"""

import polars as pl
from fasthtml.common import *
from starlette.requests import Request
from starlette.responses import JSONResponse

from quantide.data.models.stocks import StockList

app, rt = fast_app()


def get_stock_list() -> StockList:
    """获取股票列表实例"""
    return StockList()


@rt("/")
async def search_stocks(
    request: Request,
    q: str = "",
    type: str = "stock",
    limit: int = 10,
):
    """搜索个股。

    Args:
        q: 搜索关键词（代码或名称）
        type: 搜索类型，主体当前仅支持 stock
        limit: 返回结果数量限制
    """
    if type != "stock":
        return JSONResponse(
            {"code": 410, "message": "index search has been retired from the subject app"},
            status_code=410,
        )

    if not q or len(q) < 1:
        return JSONResponse({
            "code": 0,
            "message": "success",
            "data": [],
        })

    stock_list = get_stock_list()

    if stock_list.data.is_empty():
        return JSONResponse({
            "code": 0,
            "message": "success",
            "data": [],
        })

    # 构建过滤条件
    keyword = q.lower()

    # 过滤数据
    filtered = stock_list.data.filter(
        (pl.col("asset").str.to_lowercase().str.contains(keyword)) |
        (pl.col("name").str.to_lowercase().str.contains(keyword)) |
        (pl.col("pinyin").str.to_lowercase().str.contains(keyword))
    )

    # 限制返回数量
    filtered = filtered.head(limit)

    # 转换为列表
    results = []
    for row in filtered.iter_rows(named=True):
        results.append({
            "symbol": row["asset"],
            "name": row["name"],
            "pinyin": row["pinyin"],
            "list_date": row["list_date"].isoformat() if row["list_date"] else None,
        })

    return JSONResponse({
        "code": 0,
        "message": "success",
        "data": results,
    })
