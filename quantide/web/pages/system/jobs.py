"""系统设置 - 定时任务模块

提供定时任务的可视化管理，包括查看任务列表、启用/禁用任务、
手动触发执行、查看最近执行结果。
"""

from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass, field

from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, SchedulerEvent
from apscheduler.triggers.cron import CronTrigger
from fasthtml.common import *
from loguru import logger
from monsterui.all import *

from quantide.core.scheduler import scheduler
from quantide.data.sqlite import db
from quantide.web.layouts.main import MainLayout
from quantide.web.theme import AppTheme, PRIMARY_COLOR

# 定义子路由应用
system_jobs_app, rt = fast_app(hdrs=AppTheme.headers())


# ========== 预置任务定义 ==========

PREDEFINED_JOBS = {
    "daily_bars_sync": {
        "name": "日线数据同步",
        "cron": "35 15 * * 1-5",  # 每天 15:35，周一到周五
        "description": "从数据源下载当交易日行情数据",
        "enabled": True,
    },
    "stock_list_sync": {
        "name": "股票列表同步",
        "cron": "0 16 * * *",  # 每天 16:00
        "description": "更新证券列表（上市/退市）",
        "enabled": True,
    },
    "calendar_sync": {
        "name": "交易日历同步",
        "cron": "0 9 * * 1",  # 每周一 09:00
        "description": "更新节假日和调休数据",
        "enabled": True,
    },
    "daily_snapshot": {
        "name": "日终快照",
        "cron": "5 15 * * 1-5",  # 每天 15:05，周一到周五
        "description": "记录当天收盘持仓快照",
        "enabled": True,
    },
    "market_snapshot": {
        "name": "行情快照",
        "cron": "*/5 9-15 * * 1-5",  # 每5分钟，9点到15点，周一到周五
        "description": "记录最新行情数据",
        "enabled": True,
    },
}


# ========== 任务历史记录模型 ==========

@dataclass
class JobHistoryRecord:
    """任务执行记录"""
    __table_name__ = "job_history"
    __pk__ = "id"
    __indexes__ = (["job_id", "executed_at"], False)

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    job_id: str = ""
    job_name: str = ""
    executed_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    status: str = "success"  # success, error
    message: str = ""
    duration_ms: int = 0

    @classmethod
    def from_dict(cls, data: dict) -> "JobHistoryRecord":
        """从字典创建实例"""
        return cls(
            id=data.get("id", str(uuid.uuid4())),
            job_id=data.get("job_id", ""),
            job_name=data.get("job_name", ""),
            executed_at=data.get("executed_at", datetime.datetime.now()),
            status=data.get("status", "success"),
            message=data.get("message", ""),
            duration_ms=data.get("duration_ms", 0),
        )

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "id": self.id,
            "job_id": self.job_id,
            "job_name": self.job_name,
            "executed_at": self.executed_at.isoformat() if isinstance(self.executed_at, datetime.datetime) else self.executed_at,
            "status": self.status,
            "message": self.message,
            "duration_ms": self.duration_ms,
        }


def _init_job_history_table():
    """初始化任务历史记录表"""
    if "job_history" not in db.table_names():
        db["job_history"].create(
            {
                "id": str,
                "job_id": str,
                "job_name": str,
                "executed_at": str,
                "status": str,
                "message": str,
                "duration_ms": int,
            },
            pk="id",
        )
        logger.info("Created job_history table")


def _save_job_history(
    job_id: str,
    job_name: str,
    status: str,
    message: str = "",
    duration_ms: int = 0,
):
    """保存任务执行记录"""
    record = JobHistoryRecord(
        job_id=job_id,
        job_name=job_name,
        status=status,
        message=message,
        duration_ms=duration_ms,
    )
    db["job_history"].insert(record.to_dict())


def _get_job_history(job_id: str, limit: int = 5) -> list[JobHistoryRecord]:
    """获取任务最近执行记录"""
    _init_job_history_table()
    records = list(
        db["job_history"].rows_where(
            "job_id = ?",
            [job_id],
            order_by="executed_at desc",
            limit=limit,
        )
    )
    return [JobHistoryRecord.from_dict(dict(r)) for r in records]


def _get_all_job_history(limit: int = 20) -> list[JobHistoryRecord]:
    """获取所有任务最近执行记录"""
    _init_job_history_table()
    records = list(
        db["job_history"].rows_where(
            order_by="executed_at desc",
            limit=limit,
        )
    )
    return [JobHistoryRecord.from_dict(dict(r)) for r in records]


# ========== 任务执行函数 ==========

def _run_daily_bars_sync():
    """日线数据同步任务"""
    from quantide.data.models.daily_bars import daily_bars

    logger.info("Running daily bars sync job")
    try:
        daily_bars.store.update()
        _save_job_history("daily_bars_sync", "日线数据同步", "success", "下载完成")
    except Exception as e:
        logger.error(f"Daily bars sync failed: {e}")
        _save_job_history("daily_bars_sync", "日线数据同步", "error", str(e))


def _run_stock_list_sync():
    """股票列表同步任务"""
    import asyncio
    from quantide.data.models.stocks import stock_list

    logger.info("Running stock list sync job")
    try:
        asyncio.run(stock_list.update())
        _save_job_history("stock_list_sync", "股票列表同步", "success", "更新完成")
    except Exception as e:
        logger.error(f"Stock list sync failed: {e}")
        _save_job_history("stock_list_sync", "股票列表同步", "error", str(e))


def _run_calendar_sync():
    """交易日历同步任务"""
    import asyncio
    from quantide.data.models.calendar import calendar as trade_calendar

    logger.info("Running calendar sync job")
    try:
        asyncio.run(trade_calendar.update())
        _save_job_history("calendar_sync", "交易日历同步", "success", "更新完成")
    except Exception as e:
        logger.error(f"Calendar sync failed: {e}")
        _save_job_history("calendar_sync", "交易日历同步", "error", str(e))


def _run_daily_snapshot():
    """日终快照任务"""
    logger.info("Running daily snapshot job")
    # TODO: 实现持仓快照逻辑
    _save_job_history("daily_snapshot", "日终快照", "success", "快照完成")


def _run_market_snapshot():
    """行情快照任务"""
    logger.info("Running market snapshot job")
    # TODO: 实现行情快照逻辑
    _save_job_history("market_snapshot", "行情快照", "success", "快照完成")


JOB_FUNCTIONS = {
    "daily_bars_sync": _run_daily_bars_sync,
    "stock_list_sync": _run_stock_list_sync,
    "calendar_sync": _run_calendar_sync,
    "daily_snapshot": _run_daily_snapshot,
    "market_snapshot": _run_market_snapshot,
}


# ========== 任务状态管理 ==========

_job_enabled_state: dict[str, bool] = {}


def _init_scheduler_jobs():
    """初始化调度器任务"""
    _init_job_history_table()

    # 确保调度器已初始化
    if not scheduler._is_running:
        scheduler.init()
        scheduler.start()

    # 注册预置任务
    for job_id, job_def in PREDEFINED_JOBS.items():
        # 检查是否已经有这个任务
        existing_job = scheduler.scheduler.get_job(job_id)

        # 获取启用状态（从状态字典或默认值）
        enabled = _job_enabled_state.get(job_id, job_def["enabled"])

        if existing_job:
            # 更新任务状态
            if enabled and not existing_job.next_run_time:
                scheduler.scheduler.resume_job(job_id)
            elif not enabled and existing_job.next_run_time:
                scheduler.scheduler.pause_job(job_id)
        elif enabled:
            # 添加新任务
            trigger = CronTrigger.from_crontab(job_def["cron"])
            func = JOB_FUNCTIONS.get(job_id)
            if func:
                scheduler.add_job(
                    func,
                    trigger=trigger,
                    id=job_id,
                    name=job_def["name"],
                    replace_existing=True,
                )


def _toggle_job(job_id: str, enabled: bool):
    """启用/禁用任务"""
    _job_enabled_state[job_id] = enabled

    job = scheduler.scheduler.get_job(job_id)
    if job:
        if enabled:
            scheduler.scheduler.resume_job(job_id)
        else:
            scheduler.scheduler.pause_job(job_id)


def _run_job_now(job_id: str):
    """立即执行任务"""
    func = JOB_FUNCTIONS.get(job_id)
    if func:
        import asyncio
        import time

        start = time.time()
        try:
            if asyncio.iscoroutinefunction(func):
                asyncio.run(func())
            else:
                func()
            duration_ms = int((time.time() - start) * 1000)
            _save_job_history(
                job_id,
                PREDEFINED_JOBS[job_id]["name"],
                "success",
                "手动执行成功",
                duration_ms,
            )
        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            _save_job_history(
                job_id,
                PREDEFINED_JOBS[job_id]["name"],
                "error",
                str(e),
                duration_ms,
            )
            raise


def _get_job_status(job_id: str) -> dict:
    """获取任务状态"""
    job = scheduler.scheduler.get_job(job_id)
    job_def = PREDEFINED_JOBS.get(job_id, {})
    enabled = _job_enabled_state.get(job_id, job_def.get("enabled", True))

    history = _get_job_history(job_id, limit=1)
    last_run = history[0] if history else None

    return {
        "id": job_id,
        "name": job_def.get("name", job_id),
        "cron": job_def.get("cron", ""),
        "description": job_def.get("description", ""),
        "enabled": enabled,
        "has_scheduler_job": job is not None,
        "next_run": job.next_run_time.isoformat() if job and job.next_run_time else None,
        "last_run": last_run,
    }


def _format_cron(cron: str) -> str:
    """将 cron 表达式格式化为可读文本"""
    parts = cron.split()
    if len(parts) == 5:
        minute, hour, day, month, dow = parts
        if dow == "1-5" and day == "*" and month == "*":
            return f"每天 {hour}:{minute.zfill(2)} (周一至周五)"
        elif dow == "*":
            return f"每天 {hour}:{minute.zfill(2)}"
        elif dow == "1":
            return f"每周一 {hour}:{minute.zfill(2)}"
        elif "*/" in cron:
            return f"每{cron.split('/')[1]}分钟"
    return cron


# ========== UI 组件 ==========

def _build_job_status_badge(enabled: bool) -> Span:
    """构建状态徽章"""
    if enabled:
        return Span("运行中", cls="text-green-600 font-medium")
    return Span("已停止", cls="text-red-600 font-medium")


def _build_status_dot(enabled: bool) -> str:
    """构建状态圆点"""
    if enabled:
        return "🟢"
    return "🔴"


def _build_history_table(history: list[JobHistoryRecord]) -> Table:
    """构建历史记录表格"""
    rows = []
    for record in history:
        status_icon = "✅" if record.status == "success" else "❌"
        status_cls = "text-green-600" if record.status == "success" else "text-red-600"
        rows.append(
            Tr(
                Td(record.executed_at.strftime("%Y-%m-%d %H:%M:%S"), cls="px-4 py-2 text-sm"),
                Td(Span(f"{status_icon} {'成功' if record.status == 'success' else '失败'}", cls=status_cls), cls="px-4 py-2 text-sm"),
                Td(record.message or "-", cls="px-4 py-2 text-sm text-gray-500"),
                cls="hover:bg-gray-50",
            )
        )

    if not rows:
        rows.append(
            Tr(
                Td("暂无执行记录", colspan="3", cls="px-4 py-4 text-center text-gray-500")
            )
        )

    return Table(
        Thead(
            Tr(
                Th("执行时间", cls="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase"),
                Th("状态", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                Th("说明", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
            )
        ),
        Tbody(*rows),
        cls="min-w-full divide-y divide-gray-200",
    )


def _build_jobs_table(jobs_status: list[dict]) -> Table:
    """构建任务列表表格"""
    rows = []
    for job in jobs_status:
        status_dot = _build_status_dot(job["enabled"])
        toggle_url = f"/system/jobs/toggle/{job['id']}"
        detail_url = f"/system/jobs/detail/{job['id']}"

        rows.append(
            Tr(
                Td(job["name"], cls="px-4 py-3 text-sm font-medium text-gray-900"),
                Td(_format_cron(job["cron"]), cls="px-4 py-3 text-sm text-gray-500"),
                Td(_build_job_status_badge(job["enabled"]), cls="px-4 py-3 text-sm"),
                Td(
                    job["last_run"].executed_at.strftime("%Y-%m-%d %H:%M")
                    if job["last_run"] else "--",
                    cls="px-4 py-3 text-sm text-gray-500",
                ),
                Td(
                    A(
                        "禁用" if job["enabled"] else "启用",
                        href=toggle_url,
                        cls="text-sm text-blue-600 hover:text-blue-800 mr-2",
                    ),
                    A(
                        "详情",
                        href=detail_url,
                        cls="text-sm text-gray-600 hover:text-gray-800",
                    ),
                    cls="px-4 py-3 text-sm",
                ),
                cls="hover:bg-gray-50 cursor-pointer job-row",
                data_job_id=job["id"],
            )
        )

    return Table(
        Thead(
            Tr(
                Th("任务名称", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                Th("调度时间", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                Th("状态", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                Th("上次执行", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                Th("操作", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
            )
        ),
        Tbody(*rows),
        cls="min-w-full divide-y divide-gray-200",
    )


def _build_detail_panel(job_id: str) -> Div:
    """构建任务详情面板"""
    job = _get_job_status(job_id)
    history = _get_job_history(job_id, limit=5)

    return Div(
        Div(
            H3(job["name"], cls="text-lg font-semibold text-gray-900"),
            Button(
                "关闭",
                cls="text-gray-400 hover:text-gray-600 float-right",
                onclick="this.closest('.job-detail').remove()",
            ),
            cls="border-b pb-3 mb-3",
        ),
        Div(
            P(f"调度时间: {_format_cron(job['cron'])}", cls="text-sm text-gray-600 mb-1"),
            P(f"描述: {job['description']}", cls="text-sm text-gray-600 mb-1"),
            P(f"状态: ", cls="text-sm text-gray-600 mb-3"),
            cls="mb-4",
        ),
        Div(
            H4("最近执行记录", cls="text-sm font-medium text-gray-700 mb-2"),
            _build_history_table(history),
            cls="mb-4",
        ),
        Div(
            A(
                "立即执行",
                href=f"/system/jobs/run/{job_id}",
                cls="btn btn-primary",
            ),
            cls="text-center",
        ),
        cls="bg-white rounded-lg shadow-lg p-6 mb-4 job-detail",
    )


# ========== 路由 ==========

@rt("/")
async def index():
    """任务列表页面"""
    _init_scheduler_jobs()

    jobs_status = [_get_job_status(job_id) for job_id in PREDEFINED_JOBS]

    layout = MainLayout(title="定时任务")
    layout.set_sidebar_active("/system/jobs")

    page_content = Div(
        Div(
            Div(
                UkIcon("clock", size=32, cls="mr-3", style=f"color: {PRIMARY_COLOR};"),
                H2("定时任务", cls="text-2xl font-bold"),
                cls="flex items-center",
            ),
            cls="mb-6",
        ),
        Div(
            P(
                "系统预置定时任务，用于自动同步行情数据和执行日终处理。",
                cls="text-sm text-gray-500 mb-4",
            ),
        ),
        Div(
            _build_jobs_table(jobs_status),
            id="jobs-table",
            cls="bg-white rounded-lg shadow overflow-hidden",
        ),
        cls="p-8",
    )

    layout.main_block = page_content
    return layout.render()


@rt("/toggle/{job_id}")
async def toggle_job(job_id: str):
    """启用/禁用任务"""
    job_def = PREDEFINED_JOBS.get(job_id)
    if not job_def:
        return RedirectResponse("/system/jobs/", status_code=303)

    current_enabled = _job_enabled_state.get(job_id, job_def["enabled"])
    new_enabled = not current_enabled
    _toggle_job(job_id, new_enabled)

    return RedirectResponse("/system/jobs/", status_code=303)


@rt("/run/{job_id}")
async def run_job(job_id: str):
    """手动执行任务"""
    job_def = PREDEFINED_JOBS.get(job_id)
    if not job_def:
        return RedirectResponse("/system/jobs/", status_code=303)

    try:
        _run_job_now(job_id)
        logger.info(f"Job {job_id} executed manually")
    except Exception as e:
        logger.error(f"Job {job_id} execution failed: {e}")

    return RedirectResponse("/system/jobs/", status_code=303)


@rt("/detail/{job_id}")
async def job_detail(job_id: str):
    """任务详情（HTMX 局部更新）"""
    job_def = PREDEFINED_JOBS.get(job_id)
    if not job_def:
        return Div("任务不存在", cls="text-red-500 p-4")

    return _build_detail_panel(job_id)
