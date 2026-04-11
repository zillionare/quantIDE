"""系统设置 - 数据源模块

管理数据源配置，包括当前数据源查看、数据同步状态、手动同步功能。
"""

from __future__ import annotations

import asyncio
import datetime

from fasthtml.common import *
from loguru import logger
from monsterui.all import *

from quantide.config.settings import get_settings
from quantide.data.models.app_state import AppState
from quantide.data.sqlite import db
from quantide.web.layouts.main import MainLayout
from quantide.web.theme import AppTheme, PRIMARY_COLOR

# 定义子路由应用
system_datasource_app, rt = fast_app(hdrs=AppTheme.headers())


# ========== 数据获取 ==========

def _load_datasource_config() -> dict:
    """从数据库加载数据源配置"""
    try:
        row = db["app_state"].get(1)
        if row:
            state = AppState.from_dict(dict(row))
            return {
                "data_source": state.data_source,
                "tushare_token": state.tushare_token,
                "epoch": state.epoch,
            }
    except Exception as e:
        logger.warning(f"加载数据源配置失败: {e}")

    settings = get_settings()
    return {
        "data_source": settings.data_source,
        "tushare_token": settings.tushare_token if hasattr(settings, 'tushare_token') else "",
        "epoch": settings.epoch,
    }


def _get_data_status() -> dict:
    """获取各数据类型的状态"""
    status = {
        "daily_bars": {"status": "unknown", "message": "未初始化", "count": 0},
        "stock_list": {"status": "unknown", "message": "未初始化", "count": 0},
        "calendar": {"status": "unknown", "message": "未初始化", "count": 0},
    }

    try:
        from quantide.data.models.daily_bars import daily_bars

        if daily_bars.store._data is not None:
            total = daily_bars.size
            start = daily_bars.start
            end = daily_bars.end

            status["daily_bars"] = {
                "status": "ok",
                "message": f"{start} 至 {end}",
                "count": total,
            }
        else:
            status["daily_bars"] = {"status": "empty", "message": "数据为空", "count": 0}
    except Exception as e:
        status["daily_bars"] = {"status": "error", "message": str(e), "count": 0}

    try:
        from quantide.data.models.stocks import stock_list

        if stock_list._data is not None:
            total = stock_list.size
            status["stock_list"] = {
                "status": "ok",
                "message": f"共 {total} 只证券",
                "count": total,
            }
        else:
            status["stock_list"] = {"status": "empty", "message": "数据为空", "count": 0}
    except Exception as e:
        status["stock_list"] = {"status": "error", "message": str(e), "count": 0}

    try:
        from quantide.data.models.calendar import calendar as trade_calendar

        if trade_calendar._data is not None:
            total = len(trade_calendar._data)
            # 获取日历覆盖的最后日期
            last_date = trade_calendar.last_trade_date()
            status["calendar"] = {
                "status": "ok",
                "message": f"覆盖至 {last_date}" if last_date else "已加载",
                "count": total,
            }
        else:
            status["calendar"] = {"status": "empty", "message": "数据为空", "count": 0}
    except Exception as e:
        status["calendar"] = {"status": "error", "message": str(e), "count": 0}

    return status


def _get_sync_history() -> list[dict]:
    """获取最近同步记录"""
    try:
        rows = list(
            db["job_history"].rows_where(
                order_by="executed_at desc",
                limit=5,
            )
        )
        return [dict(r) for r in rows]
    except Exception:
        return []


# ========== UI 组件 ==========

def _build_status_badge(status: str) -> Span:
    """构建状态徽章"""
    if status == "ok":
        return Span("✅ 完整", cls="text-green-600 font-medium")
    elif status == "empty":
        return Span("⚠️ 空数据", cls="text-yellow-600 font-medium")
    elif status == "error":
        return Span("❌ 错误", cls="text-red-600 font-medium")
    return Span("⚪ 未知", cls="text-gray-500 font-medium")


def _build_data_status_card(status: dict) -> Div:
    """构建数据状态卡片"""
    items = [
        ("日线数据", "daily_bars", "📊"),
        ("股票列表", "stock_list", "📋"),
        ("交易日历", "calendar", "📅"),
    ]

    rows = []
    for name, key, icon in items:
        item_status = status.get(key, {"status": "unknown", "message": "", "count": 0})
        rows.append(
            Div(
                Div(
                    Span(icon, cls="text-xl mr-2"),
                    Span(name, cls="font-medium text-gray-900"),
                    cls="flex items-center",
                ),
                _build_status_badge(item_status["status"]),
                Span(item_status["message"], cls="text-sm text-gray-500 ml-4"),
                cls="flex items-center justify-between py-3 border-b border-gray-100 last:border-0",
            )
        )

    return Div(
        Div(
            H3("数据状态", cls="text-lg font-semibold text-gray-900 mb-3"),
            cls="border-b pb-2 mb-4",
        ),
        *rows,
        cls="p-6 bg-white rounded-lg shadow",
    )


def _build_config_card(config: dict) -> Div:
    """构建配置信息卡片"""
    masked_token = ""
    if config.get("tushare_token"):
        token = config["tushare_token"]
        if len(token) > 4:
            masked_token = token[:2] + "•" * (len(token) - 4) + token[-2:]
        else:
            masked_token = "•" * len(token)

    epoch_str = ""
    if config.get("epoch"):
        epoch_str = config["epoch"].strftime("%Y-%m-%d") if hasattr(config["epoch"], "strftime") else str(config["epoch"])

    return Div(
        Div(
            H3("当前数据源", cls="text-lg font-semibold text-gray-900 mb-3"),
            cls="border-b pb-2 mb-4",
        ),
        Div(
            Div(
                Span("数据源:", cls="text-sm text-gray-500"),
                Span("Tushare Pro", cls="text-sm font-medium text-gray-900 ml-2"),
                cls="flex justify-between py-2 border-b border-gray-100",
            ),
            Div(
                Span("Token:", cls="text-sm text-gray-500"),
                Span(masked_token or "-", cls="text-sm font-medium text-gray-900 ml-2"),
                cls="flex justify-between py-2 border-b border-gray-100",
            ),
            Div(
                Span("数据起始日:", cls="text-sm text-gray-500"),
                Span(epoch_str or "-", cls="text-sm font-medium text-gray-900 ml-2"),
                cls="flex justify-between py-2",
            ),
            cls="text-sm",
        ),
        cls="p-6 bg-white rounded-lg shadow",
    )


def _build_sync_history_card(history: list[dict]) -> Div:
    """构建同步记录卡片"""
    rows = []
    for record in history:
        status_icon = "✅" if record.get("status") == "success" else "❌"
        status_cls = "text-green-600" if record.get("status") == "success" else "text-red-600"
        executed_at = record.get("executed_at", "")
        if isinstance(executed_at, str):
            executed_at = executed_at[:16].replace("T", " ")

        rows.append(
            Div(
                Span(record.get("job_name", "-"), cls="font-medium text-gray-900"),
                Span(executed_at, cls="text-sm text-gray-500 ml-2"),
                Span(f"{status_icon}", cls=f"ml-2 {status_cls}"),
                cls="flex items-center justify-between py-2 border-b border-gray-100 last:border-0",
            )
        )

    if not rows:
        rows.append(
            Div(
                Span("暂无同步记录", cls="text-gray-500"),
                cls="py-4 text-center",
            )
        )

    return Div(
        Div(
            H3("最近同步记录", cls="text-lg font-semibold text-gray-900 mb-3"),
            cls="border-b pb-2 mb-4",
        ),
        *rows,
        cls="p-6 bg-white rounded-lg shadow",
    )


# ========== 路由 ==========

@rt("/")
async def index():
    """数据源页面"""
    config = _load_datasource_config()
    data_status = _get_data_status()
    sync_history = _get_sync_history()

    layout = MainLayout(title="数据源")
    layout.set_sidebar_active("/system/datasource")

    page_content = Div(
        Div(
            Div(
                UkIcon("database", size=32, cls="mr-3", style=f"color: {PRIMARY_COLOR};"),
                H2("数据源", cls="text-2xl font-bold"),
                cls="flex items-center",
            ),
            cls="mb-6",
        ),
        # 配置信息
        Div(
            _build_config_card(config),
            cls="mb-6",
        ),
        # 数据状态
        Div(
            _build_data_status_card(data_status),
            cls="mb-6",
        ),
        # 同步记录
        Div(
            _build_sync_history_card(sync_history),
            cls="mb-6",
        ),
        # 操作按钮
        Div(
            Div(
                P(
                    "手动同步将下载缺失的历史数据，可能需要较长时间。",
                    cls="text-sm text-gray-500 mb-3",
                ),
                A(
                    "手动同步全部数据",
                    href="/system/datasource/sync",
                    cls="btn btn-primary",
                ),
                cls="flex flex-col items-center",
            ),
            cls="p-4 bg-gray-50 rounded-lg",
        ),
        cls="p-8",
    )

    layout.main_block = page_content
    return layout.render()


@rt("/sync")
async def sync_data():
    """触发数据同步"""
    from quantide.data.models.daily_bars import daily_bars
    from quantide.data.models.stocks import stock_list
    from quantide.data.models.calendar import calendar as trade_calendar

    results = []
    results.append("开始同步数据...")

    # 同步交易日历
    try:
        await asyncio.to_thread(trade_calendar.update)
        results.append("✅ 交易日历同步完成")
        logger.info("Calendar sync completed")
    except Exception as e:
        results.append(f"❌ 交易日历同步失败: {e}")
        logger.error(f"Calendar sync failed: {e}")

    # 同步股票列表
    try:
        await asyncio.to_thread(stock_list.update)
        results.append("✅ 股票列表同步完成")
        logger.info("Stock list sync completed")
    except Exception as e:
        results.append(f"❌ 股票列表同步失败: {e}")
        logger.error(f"Stock list sync failed: {e}")

    # 同步日线数据
    try:
        await asyncio.to_thread(daily_bars.store.update)
        results.append("✅ 日线数据同步完成")
        logger.info("Daily bars sync completed")
    except Exception as e:
        results.append(f"❌ 日线数据同步失败: {e}")
        logger.error(f"Daily bars sync failed: {e}")

    results.append("同步完成！")

    layout = MainLayout(title="数据源")
    layout.set_sidebar_active("/system/datasource")

    page_content = Div(
        Div(
            Div(
                UkIcon("database", size=32, cls="mr-3", style=f"color: {PRIMARY_COLOR};"),
                H2("数据源", cls="text-2xl font-bold"),
                cls="flex items-center",
            ),
            cls="mb-6",
        ),
        Div(
            H3("同步结果", cls="text-lg font-semibold text-gray-900 mb-3"),
            Div(
                *[P(r, cls="py-1") for r in results],
                cls="p-4 bg-gray-50 rounded-lg mb-4",
            ),
            A(
                "返回",
                href="/system/datasource/",
                cls="btn btn-primary",
            ),
            cls="mb-6",
        ),
        cls="p-8",
    )

    layout.main_block = page_content
    return layout.render()
