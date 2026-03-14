"""指数管理 API"""

from fasthtml.common import *
from starlette.requests import Request
from starlette.responses import JSONResponse

from pyqmt.data.dal.index_dal import IndexDAL
from pyqmt.data.models.index import Index
from pyqmt.data.sqlite import db

app, rt = fast_app()


def get_index_dal() -> IndexDAL:
    """获取 IndexDAL 实例"""
    return IndexDAL(db)


def index_to_dict(index: Index) -> dict:
    """将 Index 对象转换为字典"""
    return {
        "symbol": index.symbol,
        "name": index.name,
        "index_type": index.index_type,
        "category": index.category,
        "publisher": index.publisher,
        "base_date": index.base_date.isoformat() if index.base_date else None,
        "base_point": index.base_point,
        "list_date": index.list_date.isoformat() if index.list_date else None,
        "description": index.description,
        "updated_at": index.updated_at.isoformat() if index.updated_at else None,
    }


@rt("/")
async def list_indices(
    request: Request,
    index_type: str | None = None,
    category: str | None = None,
    page: int = 1,
    size: int = 20,
):
    """列出指数"""
    dal = get_index_dal()
    indices = dal.list_indices(index_type=index_type, category=category)

    # 分页
    total = len(indices)
    start = (page - 1) * size
    end = start + size
    items = [index_to_dict(index) for index in indices[start:end]]

    return JSONResponse({
        "code": 0,
        "message": "success",
        "data": {
            "items": items,
            "total": total,
            "page": page,
            "size": size,
        },
    })


@rt("/{symbol}")
async def get_index(request: Request, symbol: str):
    """获取指数详情"""
    dal = get_index_dal()
    index = dal.get_index(symbol)

    if not index:
        return JSONResponse(
            {"code": 404, "message": "Index not found"},
            status_code=404,
        )

    return JSONResponse({
        "code": 0,
        "message": "success",
        "data": index_to_dict(index),
    })
