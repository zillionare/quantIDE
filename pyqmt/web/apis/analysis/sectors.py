"""板块管理 API"""

import datetime
from pathlib import Path

from fasthtml.common import *
from starlette.requests import Request
from starlette.responses import JSONResponse

from pyqmt.data.dal.sector_dal import SectorDAL
from pyqmt.data.models.sector import Sector
from pyqmt.data.sqlite import db

app, rt = fast_app()


def get_sector_dal() -> SectorDAL:
    """获取 SectorDAL 实例"""
    return SectorDAL(db)


def sector_to_dict(sector: Sector, stock_count: int = 0) -> dict:
    """将 Sector 对象转换为字典"""
    return {
        "id": sector.id,
        "name": sector.name,
        "sector_type": sector.sector_type,
        "source": sector.source,
        "description": sector.description,
        "stock_count": stock_count,
        "created_at": sector.created_at.isoformat() if sector.created_at else None,
        "updated_at": sector.updated_at.isoformat() if sector.updated_at else None,
    }


@rt("/")
async def list_sectors(
    request: Request,
    sector_type: str | None = None,
    source: str | None = None,
    page: int = 1,
    size: int = 20,
):
    """列出板块"""
    dal = get_sector_dal()
    sectors = dal.list_sectors(sector_type=sector_type, source=source)

    # 计算成分股数量
    result = []
    for sector in sectors:
        stocks = dal.get_sector_stocks(sector.id)
        result.append(sector_to_dict(sector, len(stocks)))

    # 分页
    total = len(result)
    start = (page - 1) * size
    end = start + size
    items = result[start:end]

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


@rt("/", methods=["POST"])
async def create_sector(request: Request):
    """创建板块"""
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(
            {"code": 400, "message": "Invalid JSON"},
            status_code=400,
        )

    name = data.get("name")
    if not name:
        return JSONResponse(
            {"code": 400, "message": "Name is required"},
            status_code=400,
        )

    sector_type = data.get("sector_type", "custom")
    if sector_type not in ("custom", "industry", "concept"):
        return JSONResponse(
            {"code": 400, "message": "Invalid sector_type"},
            status_code=400,
        )

    # 生成ID（自定义板块）
    import uuid
    sector_id = f"custom_{uuid.uuid4().hex[:8]}"

    sector = Sector(
        id=sector_id,
        name=name,
        sector_type=sector_type,
        source="user",
        description=data.get("description", ""),
    )

    dal = get_sector_dal()
    try:
        dal.create_sector(sector)
        return JSONResponse({
            "code": 0,
            "message": "success",
            "data": sector_to_dict(sector),
        })
    except Exception as e:
        return JSONResponse(
            {"code": 500, "message": f"Failed to create sector: {e}"},
            status_code=500,
        )


@rt("/{sector_id}")
async def get_sector(request: Request, sector_id: str):
    """获取板块详情"""
    dal = get_sector_dal()
    sector = dal.get_sector(sector_id)

    if not sector:
        return JSONResponse(
            {"code": 404, "message": "Sector not found"},
            status_code=404,
        )

    stocks = dal.get_sector_stocks(sector_id)
    return JSONResponse({
        "code": 0,
        "message": "success",
        "data": sector_to_dict(sector, len(stocks)),
    })


@rt("/{sector_id}", methods=["PUT"])
async def update_sector(request: Request, sector_id: str):
    """更新板块"""
    dal = get_sector_dal()
    sector = dal.get_sector(sector_id)

    if not sector:
        return JSONResponse(
            {"code": 404, "message": "Sector not found"},
            status_code=404,
        )

    # 只允许更新用户自定义板块
    if sector.source != "user":
        return JSONResponse(
            {"code": 403, "message": "Cannot update system sector"},
            status_code=403,
        )

    try:
        data = await request.json()
    except Exception:
        return JSONResponse(
            {"code": 400, "message": "Invalid JSON"},
            status_code=400,
        )

    if "name" in data:
        sector.name = data["name"]
    if "description" in data:
        sector.description = data["description"]

    try:
        dal.update_sector(sector)
        stocks = dal.get_sector_stocks(sector_id)
        return JSONResponse({
            "code": 0,
            "message": "success",
            "data": sector_to_dict(sector, len(stocks)),
        })
    except Exception as e:
        return JSONResponse(
            {"code": 500, "message": f"Failed to update sector: {e}"},
            status_code=500,
        )


@rt("/{sector_id}", methods=["DELETE"])
async def delete_sector(request: Request, sector_id: str):
    """删除板块"""
    dal = get_sector_dal()
    sector = dal.get_sector(sector_id)

    if not sector:
        return JSONResponse(
            {"code": 404, "message": "Sector not found"},
            status_code=404,
        )

    # 只允许删除用户自定义板块
    if sector.source != "user":
        return JSONResponse(
            {"code": 403, "message": "Cannot delete system sector"},
            status_code=403,
        )

    try:
        dal.delete_sector(sector_id)
        return JSONResponse({
            "code": 0,
            "message": "success",
        })
    except Exception as e:
        return JSONResponse(
            {"code": 500, "message": f"Failed to delete sector: {e}"},
            status_code=500,
        )


@rt("/{sector_id}/stocks")
async def get_sector_stocks(request: Request, sector_id: str):
    """获取板块成分股"""
    dal = get_sector_dal()
    sector = dal.get_sector(sector_id)

    if not sector:
        return JSONResponse(
            {"code": 404, "message": "Sector not found"},
            status_code=404,
        )

    stocks = dal.get_sector_stocks(sector_id)
    return JSONResponse({
        "code": 0,
        "message": "success",
        "data": [
            {
                "symbol": stock.symbol,
                "name": stock.name,
                "weight": stock.weight,
                "added_at": stock.added_at.isoformat() if stock.added_at else None,
            }
            for stock in stocks
        ],
    })


@rt("/{sector_id}/stocks", methods=["POST"])
async def add_sector_stock(request: Request, sector_id: str):
    """添加成分股"""
    dal = get_sector_dal()
    sector = dal.get_sector(sector_id)

    if not sector:
        return JSONResponse(
            {"code": 404, "message": "Sector not found"},
            status_code=404,
        )

    try:
        data = await request.json()
    except Exception:
        return JSONResponse(
            {"code": 400, "message": "Invalid JSON"},
            status_code=400,
        )

    symbol = data.get("symbol")
    if not symbol:
        return JSONResponse(
            {"code": 400, "message": "Symbol is required"},
            status_code=400,
        )

    name = data.get("name", "")
    weight = data.get("weight", 0.0)

    try:
        if dal.add_stock_to_sector(sector_id, symbol, name, weight):
            return JSONResponse({
                "code": 0,
                "message": "success",
            })
        else:
            return JSONResponse(
                {"code": 500, "message": "Failed to add stock"},
                status_code=500,
            )
    except Exception as e:
        return JSONResponse(
            {"code": 500, "message": f"Failed to add stock: {e}"},
            status_code=500,
        )


@rt("/{sector_id}/stocks/{symbol}", methods=["DELETE"])
async def remove_sector_stock(request: Request, sector_id: str, symbol: str):
    """删除成分股"""
    dal = get_sector_dal()
    sector = dal.get_sector(sector_id)

    if not sector:
        return JSONResponse(
            {"code": 404, "message": "Sector not found"},
            status_code=404,
        )

    try:
        if dal.remove_stock_from_sector(sector_id, symbol):
            return JSONResponse({
                "code": 0,
                "message": "success",
            })
        else:
            return JSONResponse(
                {"code": 500, "message": "Failed to remove stock"},
                status_code=500,
            )
    except Exception as e:
        return JSONResponse(
            {"code": 500, "message": f"Failed to remove stock: {e}"},
            status_code=500,
        )


@rt("/{sector_id}/import", methods=["POST"])
async def import_stocks(request: Request, sector_id: str):
    """从文件导入成分股"""
    dal = get_sector_dal()
    sector = dal.get_sector(sector_id)

    if not sector:
        return JSONResponse(
            {"code": 404, "message": "Sector not found"},
            status_code=404,
        )

    try:
        data = await request.json()
    except Exception:
        return JSONResponse(
            {"code": 400, "message": "Invalid JSON"},
            status_code=400,
        )

    file_path = data.get("file_path")
    if not file_path:
        return JSONResponse(
            {"code": 400, "message": "file_path is required"},
            status_code=400,
        )

    if not Path(file_path).exists():
        return JSONResponse(
            {"code": 404, "message": "File not found"},
            status_code=404,
        )

    try:
        success, failed, failed_symbols = dal.import_stocks_from_file(
            sector_id, file_path
        )
        return JSONResponse({
            "code": 0,
            "message": "success",
            "data": {
                "success": success,
                "failed": failed,
                "failed_symbols": failed_symbols,
            },
        })
    except Exception as e:
        return JSONResponse(
            {"code": 500, "message": f"Failed to import stocks: {e}"},
            status_code=500,
        )


@rt("/stock/{symbol}")
async def get_stock_sectors(request: Request, symbol: str):
    """获取个股所属板块"""
    dal = get_sector_dal()
    sectors = dal.get_stock_sectors(symbol)

    return JSONResponse({
        "code": 0,
        "message": "success",
        "data": [
            {
                "id": sector.id,
                "name": sector.name,
                "sector_type": sector.sector_type,
                "source": sector.source,
            }
            for sector in sectors
        ],
    })
