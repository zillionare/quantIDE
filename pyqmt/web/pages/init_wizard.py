"""应用程序初始化向导页面

使用 MonsterUI 实现多步骤初始化向导。
"""

import asyncio
import datetime
import json
from typing import Any

from fasthtml.common import *
from loguru import logger
from monsterui.all import *
from starlette.responses import StreamingResponse

from pyqmt.data.services import IndexSyncService, SectorSyncService, StockSyncService
from pyqmt.data.dal.index_dal import IndexDAL
from pyqmt.data.dal.sector_dal import SectorDAL
from pyqmt.data.models.calendar import Calendar
from pyqmt.data.models.daily_bars import daily_bars
from pyqmt.data.models.stocks import stock_list
from pyqmt.data.sqlite import db
from pyqmt.service.init_wizard import init_wizard
from pyqmt.web.layouts.base import BaseLayout

from pyqmt.web.theme import AppTheme, PRIMARY_COLOR

init_wizard_app, rt = fast_app(hdrs=AppTheme.headers())


# ========== 全局同步状态 ==========
_sync_status = {
    "is_running": False,
    "current_task": "",
    "progress": 0,
    "message": "",
    "completed": False,
    "error": None,
}


def _update_sync_status(progress: int, message: str, completed: bool = False, error: str | None = None):
    """更新同步状态"""
    global _sync_status
    _sync_status["progress"] = progress
    _sync_status["message"] = message
    _sync_status["completed"] = completed
    _sync_status["error"] = error
    logger.info(f"同步进度: {progress}% - {message}")


# ========== 步骤指示器组件 ==========

# 主色调配置
PRIMARY_COLOR = "#D13527"


def StepIndicator(current_step: int, steps: list[dict]):
    """步骤指示器组件（竖状布局）

    使用自定义样式的竖状步骤条，主色调为 #D13527。

    Args:
        current_step: 当前步骤（1-5）
        steps: 步骤列表，每个步骤包含 id, name, completed
    """
    step_items = []
    for i, step in enumerate(steps, 1):
        is_active = i == current_step
        is_completed = step.get("completed", False)

        # 确定步骤样式
        if is_active:
            # 当前步骤：使用主色调
            text_style = f"color: {PRIMARY_COLOR}; font-weight: bold;"
            circle_bg = PRIMARY_COLOR
        elif is_completed:
            # 已完成步骤：主色调
            text_style = f"color: {PRIMARY_COLOR};"
            circle_bg = PRIMARY_COLOR
        else:
            # 未开始步骤：灰色
            text_style = "color: #9ca3af;"
            circle_bg = "#e5e7eb"

        step_items.append(
            Li(
                Div(
                    # 步骤编号圆圈 - 使用固定宽高比确保圆形
                    Span(
                        str(i),
                        cls="step-number flex-shrink-0",
                        style=f"background: {circle_bg}; color: white; width: 32px; height: 32px; min-width: 32px; min-height: 32px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 14px; font-weight: bold; margin-right: 12px; flex-shrink: 0;",
                    ),
                    # 步骤名称 - 不换行
                    Span(step["name"], style=f"{text_style} white-space: nowrap;"),
                    cls="flex items-center py-3",
                ),
                cls="step",
            )
        )

    return Ul(*step_items, cls="steps-vertical list-none p-0 m-0 w-full")


# ========== 步骤内容组件 ==========

def Step1_Welcome():
    """步骤1：欢迎页面"""
    return Div(
        H3("欢迎使用 PyQMT", cls="mb-6", style=f"color: {PRIMARY_COLOR};"),
        P(
            "PyQMT 是一个基于 Python 的量化交易系统。",
            cls="text-gray-600 mb-4",
        ),
        P(
            "在开始使用之前，我们需要完成一些初始化配置，包括：",
            cls="text-gray-600 mb-4",
        ),
        Ul(
            Li("配置数据源（Tushare、QMT）"),
            Li("设置定时任务时间"),
            Li("下载历史行情数据"),
            cls="list-disc pl-6 mb-6 text-gray-600",
        ),
        P(
            "整个初始化过程大约需要 5-10 分钟，取决于您选择下载的历史数据范围。",
            cls="text-gray-500 text-sm mb-6",
        ),
        cls="py-4",
    )


def Step2_DataSource(state: dict | None = None):
    """步骤2：数据源配置"""
    state = state or {}

    return Div(
        H4("配置数据源", cls="mb-4", style=f"color: {PRIMARY_COLOR};"),
        # Tushare 配置
        Card(
            CardHeader(H5("Tushare 配置", cls="text-lg font-semibold")),
            CardBody(
                P(
                    "Tushare 是数据源，用于获取股票行情、财务数据等。",
                    cls="text-sm text-gray-600 mb-4",
                ),
                LabelInput(
                    label="Tushare Token",
                    name="tushare_token",
                    value=state.get("tushare_token", ""),
                    placeholder="请输入您的 Tushare Pro Token",
                    required=True,
                ),
                P(
                    A(
                        "获取 Tushare Token",
                        href="https://tushare.pro/register",
                        target="_blank",
                        cls="text-sm text-primary",
                    ),
                    cls="mt-2",
                ),
            ),
            cls="mb-4",
        ),
        # QMT 配置（实盘交易）
        Card(
            CardHeader(H5("QMT 实盘配置", cls="text-lg font-semibold")),
            CardBody(
                P(
                    "QMT 是实盘交易执行端。如果您不使用实盘交易，可以跳过此步骤。",
                    cls="text-sm text-gray-600 mb-4",
                ),
                # 隐藏字段：固定为实盘类型
                Input(
                    type="hidden",
                    name="qmt_account_type",
                    value="live",
                ),
                LabelInput(
                    label="QMT 账号 ID",
                    name="qmt_account_id",
                    value=state.get("qmt_account_id", ""),
                    placeholder="请输入 QMT 实盘账号 ID",
                ),
                LabelInput(
                    label="QMT 安装路径",
                    name="qmt_path",
                    value=state.get("qmt_path", ""),
                    placeholder="例如: C:/国金证券QMT交易端",
                ),
            ),
            cls="mb-4",
        ),
        cls="max-w-2xl mx-auto",
    )


def Step3_Schedule(state: dict | None = None):
    """步骤3：任务调度配置"""
    state = state or {}

    return Div(
        H4("配置任务调度时间", cls="mb-4", style=f"color: {PRIMARY_COLOR};"),
        P(
            "设置各项数据同步任务的执行时间。建议保持默认设置。",
            cls="text-gray-600 mb-6",
        ),
        Card(
            CardBody(
                Grid(
                    # 上午任务
                    Div(
                        H6("上午任务", cls="font-semibold mb-3"),
                        LabelInput(
                            label="涨跌停刷新时间",
                            name="limit_refresh_time",
                            type="time",
                            value=state.get("limit_refresh_time", "09:00"),
                            cls="mb-3",
                        ),
                        LabelInput(
                            label="复权因子获取时间",
                            name="adj_factor_time",
                            type="time",
                            value=state.get("adj_factor_time", "09:20"),
                        ),
                    ),
                    # 下午任务
                    Div(
                        H6("下午任务", cls="font-semibold mb-3"),
                        LabelInput(
                            label="日线数据获取时间",
                            name="daily_fetch_time",
                            type="time",
                            value=state.get("daily_fetch_time", "16:00"),
                            cls="mb-3",
                        ),
                        LabelInput(
                            label="板块数据同步时间",
                            name="sector_sync_time",
                            type="time",
                            value=state.get("sector_sync_time", "19:00"),
                        ),
                    ),
                    # 晚间任务
                    Div(
                        H6("晚间任务", cls="font-semibold mb-3"),
                        LabelInput(
                            label="指数数据同步时间",
                            name="index_sync_time",
                            type="time",
                            value=state.get("index_sync_time", "19:30"),
                        ),
                    ),
                    cols=3,
                    cls="gap-4",
                ),
            ),
            cls="mb-4",
        ),
        cls="max-w-3xl mx-auto",
    )


def Step4_DownloadData(state: dict | None = None):
    """步骤4：下载历史数据"""
    state = state or {}

    # 计算默认起始日期（2年前）
    default_start = (datetime.date.today() - datetime.timedelta(days=730)).strftime(
        "%Y-%m-%d"
    )

    return Div(
        H4("下载历史数据", cls="mb-4", style=f"color: {PRIMARY_COLOR};"),
        P(
            "选择要下载的历史数据范围。数据量越大，下载时间越长。",
            cls="text-gray-600 mb-6",
        ),
        Card(
            CardBody(
                Div(
                    FormLabel("数据起始日期", required=True),
                    Input(
                        type="date",
                        name="history_start_date",
                        value=state.get("history_start_date", default_start),
                        cls="uk-input",
                    ),
                    P(
                        "建议至少下载 1 年的历史数据，回测建议 3 年以上。",
                        cls="text-sm text-gray-500 mt-1",
                    ),
                    cls="mb-4",
                ),
                # 下载进度区域（初始隐藏）
                Div(
                    id="download-progress",
                    cls="hidden",
                ),
            ),
            cls="mb-4",
        ),
        # 数据说明
        Card(
            CardHeader(H6("将下载的数据", cls="font-semibold")),
            CardBody(
                Ul(
                    Li("股票日线行情（开高低收、成交量、复权因子）"),
                    Li("涨跌停价格数据"),
                    Li("ST 股票标记"),
                    Li("板块数据"),
                    Li("指数数据"),
                    cls="list-disc pl-6 text-sm text-gray-600",
                ),
            ),
            cls="mb-4",
        ),
        cls="max-w-2xl mx-auto",
    )


def Step5_Complete():
    """步骤5：完成页面"""
    return Div(
        Div(
            UkIcon("check-circle", width=64, height=64, cls="text-green-500"),
            cls="mb-6",
        ),
        H3("初始化完成！", cls="mb-4", style=f"color: {PRIMARY_COLOR};"),
        P(
            "您的 PyQMT 系统已完成初始化配置。",
            cls="text-gray-600 mb-4",
        ),
        P(
            "系统将自动开始执行数据同步任务，您现在可以：",
            cls="text-gray-600 mb-4",
        ),
        Ul(
            Li("查看实时行情"),
            Li("创建和运行策略"),
            Li("进行回测或实盘交易"),
            Li("分析历史数据"),
            cls="list-disc pl-6 mb-6 text-gray-600",
        ),
        # 同步进度对话框容器
        Div(id="sync-dialog", cls="mt-6"),
        cls="py-4",
    )


# ========== 导航按钮组件 ==========

def WizardButtons(current_step: int, total_steps: int = 5):
    """向导导航按钮 - 上一步在左，下一步在右"""
    left_buttons = []
    right_buttons = []

    # 上一步按钮（除了第一步）- 放在左侧
    if current_step > 1:
        left_buttons.append(
            Button(
                "上一步",
                cls="btn px-6 py-2 rounded",
                style="background: #f3f4f6; color: #374151; border: 1px solid #d1d5db;",
                hx_post=f"/init-wizard/step/{current_step - 1}",
                hx_target="#wizard-form-container",
                hx_swap="innerHTML",
                hx_include="[name]",
            )
        )

    # 下一步/完成按钮 - 放在右侧
    if current_step < total_steps:
        right_buttons.append(
            Button(
                "下一步",
                cls="btn px-6 py-2 rounded",
                style=f"background: {PRIMARY_COLOR}; color: white; border: none;",
                hx_post=f"/init-wizard/step/{current_step + 1}",
                hx_target="#wizard-form-container",
                hx_swap="innerHTML",
                hx_include="[name]",
            )
        )
    else:
        right_buttons.append(
            Button(
                "开始同步",
                cls="btn px-6 py-2 rounded",
                style=f"background: {PRIMARY_COLOR}; color: white; border: none;",
                hx_post="/init-wizard/complete",
                hx_target="#sync-dialog",
                hx_swap="innerHTML",
            )
        )

    return Div(
        Div(*left_buttons, cls="flex gap-2"),
        Div(*right_buttons, cls="flex gap-2"),
        cls="flex justify-between mt-12 pt-6 border-t border-gray-200",
    )


# ========== 主页面 ==========

def InitWizardPage(step: int = 1, form_data: dict | None = None):
    """初始化向导主页面

    Args:
        step: 当前步骤（1-5）
        form_data: 表单数据
    """
    # 获取进度信息
    progress = init_wizard.get_progress()
    steps = progress["steps"]

    # 获取当前状态
    state = init_wizard.get_state()
    state_dict = state.to_dict()

    # 合并表单数据
    if form_data:
        state_dict.update(form_data)

    # 根据步骤渲染内容
    step_content_map = {
        1: Step1_Welcome(),
        2: Step2_DataSource(state_dict),
        3: Step3_Schedule(state_dict),
        4: Step4_DownloadData(state_dict),
        5: Step5_Complete(),
    }

    step_content = step_content_map.get(step, Step1_Welcome())

    return BaseLayout(
        Div(
            # 主内容区
            Div(
                # 左侧步骤指示器
                Div(
                    StepIndicator(step, steps),
                    cls="w-48 flex-shrink-0",
                ),
                # 右侧内容区 - 整个表单作为 HTMX 替换目标
                Div(
                    Form(
                        # 步骤内容
                        Div(step_content, id="wizard-content"),
                        # 导航按钮
                        WizardButtons(step),
                        cls="flex-1",
                    ),
                    id="wizard-form-container",
                    cls="flex-1 pl-8",
                ),
                cls="flex",
            ),
            cls="max-w-5xl mx-auto py-8 px-4",
        ),
        page_title="系统初始化 - PyQMT",
    )


# ========== 路由处理 ==========


@rt("/")
async def get(request: Request):
    """初始化向导主页"""
    # 检查是否有 force 参数，强制显示向导
    force = request.query_params.get("force", "false").lower() == "true"

    if not force:
        try:
            # 检查是否已完成初始化
            is_init = init_wizard.is_initialized()
            if is_init:
                # 已初始化，重定向到首页
                return RedirectResponse("/")
        except RuntimeError as e:
            # 数据库未初始化，继续显示向导
            logger.warning(f"检查初始化状态时出错：{e}")

    # force=true 时，保留已有配置，不重置状态
    # 只在没有 force 参数且首次初始化时，才调用 start_initialization
    if not force:
        try:
            init_wizard.start_initialization()
        except RuntimeError as e:
            logger.warning(f"开始初始化流程时出错：{e}")

    # 获取当前状态（包含已有配置）
    state = init_wizard.get_state()
    
    # 显示第一步，带入已有配置
    return InitWizardPage(step=1, form_data=state.to_dict())


@rt("/step/{step}")
async def handle_step(request: Request, step: int):
    """处理步骤导航"""
    # 获取表单数据
    form_data = await request.form()
    form_dict = dict(form_data)

    # 保存当前步骤的配置
    if step == 2:
        # 保存数据源配置
        init_wizard.save_data_source_config(
            tushare_token=str(form_dict.get("tushare_token", "")),
            qmt_account_id=str(form_dict.get("qmt_account_id", "")),
            qmt_account_type=str(form_dict.get("qmt_account_type", "live")),
            qmt_path=str(form_dict.get("qmt_path", "")),
        )
    elif step == 3:
        # 保存调度配置
        init_wizard.save_schedule_config(
            daily_fetch_time=str(form_dict.get("daily_fetch_time", "16:00")),
            limit_refresh_time=str(form_dict.get("limit_refresh_time", "09:00")),
            adj_factor_time=str(form_dict.get("adj_factor_time", "09:20")),
            sector_sync_time=str(form_dict.get("sector_sync_time", "19:00")),
            index_sync_time=str(form_dict.get("index_sync_time", "19:30")),
        )
    elif step == 4:
        # 保存历史数据配置
        start_date_str = form_dict.get("history_start_date", "")
        if start_date_str:
            try:
                start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
                init_wizard.save_history_config(start_date)
            except ValueError:
                logger.warning(f"无效的日期格式：{start_date_str}")

    # 更新当前步骤
    init_wizard.update_step(step)

    # 只返回步骤内容和导航按钮，不返回整个页面
    state_dict = init_wizard.get_state().to_dict()
    state_dict.update(form_dict)

    step_content_map = {
        1: Step1_Welcome(),
        2: Step2_DataSource(state_dict),
        3: Step3_Schedule(state_dict),
        4: Step4_DownloadData(state_dict),
        5: Step5_Complete(),
    }

    step_content = step_content_map.get(step, Step1_Welcome())

    # 返回与初始页面完全相同的结构
    return Div(
        Form(
            Div(step_content, id="wizard-content"),
            WizardButtons(step),
            cls="flex-1",
        ),
        id="wizard-form-container",
        cls="flex-1 pl-8",
    )


async def _run_data_sync(start_date: datetime.date | None = None):
    """在后台运行数据同步任务

    Args:
        start_date: 历史数据起始日期
    """
    global _sync_status
    _sync_status["is_running"] = True
    _sync_status["completed"] = False
    _sync_status["error"] = None

    try:
        # 确保数据层已初始化
        _update_sync_status(2, "正在初始化数据层...")
        from pyqmt.data import init_data
        from pyqmt.config import cfg

        init_data(cfg.home, init_db=True)

        # 重新导入已初始化的对象
        from pyqmt.data.models.stocks import stock_list as sl
        from pyqmt.data.models.daily_bars import daily_bars as dbars
        from pyqmt.data.models.calendar import calendar as cal

        # 初始化 DAL 和服务
        sector_dal = SectorDAL(db)
        index_dal = IndexDAL(db)

        sector_sync = SectorSyncService(sector_dal)
        index_sync = IndexSyncService(index_dal)

        # 获取日历
        calendar = cal

        # 创建股票同步服务
        stock_sync = StockSyncService(sl, dbars.store, calendar)

        # 1. 同步股票列表 (0-20%)
        _update_sync_status(5, "正在连接数据源...")
        await asyncio.sleep(0.5)

        _update_sync_status(10, "正在同步股票列表...")
        try:
            stock_count = await asyncio.to_thread(stock_sync.sync_stock_list)
            _update_sync_status(20, f"股票列表同步完成，共 {stock_count} 只")
        except Exception as e:
            logger.error(f"同步股票列表失败: {e}")
            _update_sync_status(20, f"股票列表同步失败: {e}")

        await asyncio.sleep(0.5)

        # 2. 同步板块数据 (20-40%)
        _update_sync_status(25, "正在同步板块列表...")
        try:
            sector_result = await asyncio.to_thread(sector_sync.sync_all_sectors)
            total_sectors = sector_result.get("industry", 0) + sector_result.get("concept", 0)
            _update_sync_status(35, f"板块列表同步完成，共 {total_sectors} 个")
        except Exception as e:
            logger.error(f"同步板块列表失败: {e}")
            _update_sync_status(35, f"板块列表同步失败: {e}")

        await asyncio.sleep(0.5)

        # 3. 同步板块成分股 (40-50%)
        _update_sync_status(40, "正在同步板块成分股...")
        try:
            await asyncio.to_thread(sector_sync.sync_all_sector_stocks)
            _update_sync_status(50, "板块成分股同步完成")
        except Exception as e:
            logger.error(f"同步板块成分股失败: {e}")
            _update_sync_status(50, f"板块成分股同步失败: {e}")

        await asyncio.sleep(0.5)

        # 4. 同步指数列表 (50-60%)
        _update_sync_status(55, "正在同步指数列表...")
        try:
            index_count = await asyncio.to_thread(index_sync.sync_index_list)
            _update_sync_status(60, f"指数列表同步完成，共 {index_count} 个")
        except Exception as e:
            logger.error(f"同步指数列表失败: {e}")
            _update_sync_status(60, f"指数列表同步失败: {e}")

        await asyncio.sleep(0.5)

        # 5. 同步历史行情数据 (60-95%)
        _update_sync_status(65, "正在同步历史行情数据...")
        try:
            if start_date:
                # 全量同步
                await asyncio.to_thread(stock_sync.sync_daily_bars, start_date)
            else:
                # 只同步最近的数据
                await asyncio.to_thread(stock_sync.sync_daily_bars)
            _update_sync_status(85, "历史行情数据同步完成")
        except Exception as e:
            logger.error(f"同步历史行情数据失败: {e}")
            _update_sync_status(85, f"历史行情数据同步失败: {e}")

        await asyncio.sleep(0.5)

        # 6. 同步指数行情 (85-95%)
        _update_sync_status(90, "正在同步指数行情...")
        try:
            await asyncio.to_thread(index_sync.sync_all_index_bars)
            _update_sync_status(95, "指数行情同步完成")
        except Exception as e:
            logger.error(f"同步指数行情失败: {e}")
            _update_sync_status(95, f"指数行情同步失败: {e}")

        await asyncio.sleep(0.5)

        # 7. 完成
        _update_sync_status(100, "同步完成！", completed=True)

        # 标记初始化完成
        init_wizard.complete_initialization()
        logger.info("数据同步全部完成")

    except Exception as e:
        logger.error(f"数据同步过程中发生错误: {e}")
        _update_sync_status(_sync_status["progress"], f"同步失败: {e}", error=str(e))
    finally:
        _sync_status["is_running"] = False


@rt("/complete")
async def handle_complete():
    """完成初始化 - 启动数据同步"""
    global _sync_status

    try:
        # 获取历史数据配置
        state = init_wizard.get_state()
        start_date = state.history_start_date

        # 重置同步状态
        _sync_status = {
            "is_running": True,
            "current_task": "",
            "progress": 0,
            "message": "正在初始化...",
            "completed": False,
            "error": None,
        }

        # 启动后台同步任务
        asyncio.create_task(_run_data_sync(start_date))
        logger.info(f"初始化向导完成，开始数据同步，起始日期: {start_date}")

        # 返回同步进度对话框
        return SyncProgressDialog()
    except Exception as e:
        logger.error(f"完成初始化失败: {e}")
        return Div(
            P(f"初始化失败: {e}", cls="text-red-500"),
            cls="text-center p-4",
        )


def SyncProgressDialog():
    """同步进度对话框"""
    return Div(
        # 遮罩层
        Div(
            cls="fixed inset-0 bg-black bg-opacity-50 z-40",
        ),
        # 对话框
        Div(
            Div(
                H4("正在同步数据", cls="text-lg font-semibold mb-4", style=f"color: {PRIMARY_COLOR};"),
                P("正在初始化系统并同步必要数据，请稍候...", cls="text-gray-600 mb-4"),
                # 进度条
                Div(
                    Div(
                        cls="h-2 bg-gray-200 rounded-full overflow-hidden",
                    ),
                    Div(
                        id="sync-progress-bar",
                        cls="h-2 bg-red-600 rounded-full -mt-2 transition-all duration-300",
                        style="width: 0%;",
                    ),
                    cls="mb-4",
                ),
                # 状态文本
                P(
                    "正在连接数据源...",
                    id="sync-status",
                    cls="text-sm text-gray-500 mb-4",
                ),
                # 进入系统按钮（初始隐藏）
                Button(
                    "进入系统",
                    id="enter-system-btn",
                    cls="btn px-6 py-2 rounded hidden",
                    style=f"background: {PRIMARY_COLOR}; color: white; border: none;",
                    onclick="window.location.href='/'",
                ),
                cls="bg-white rounded-lg shadow-xl p-6 max-w-md w-full mx-4",
            ),
            cls="fixed inset-0 flex items-center justify-center z-50",
        ),
        # SSE 连接脚本
        Script("""
            (function() {
                const progressBar = document.getElementById('sync-progress-bar');
                const statusText = document.getElementById('sync-status');
                const enterBtn = document.getElementById('enter-system-btn');

                // 创建 EventSource 连接
                const evtSource = new EventSource('/init-wizard/sync-progress');

                evtSource.onmessage = function(event) {
                    try {
                        const data = JSON.parse(event.data);

                        // 更新进度条
                        progressBar.style.width = data.progress + '%';
                        statusText.textContent = data.message;

                        // 检查是否完成
                        if (data.completed) {
                            enterBtn.classList.remove('hidden');
                            evtSource.close();
                        }

                        // 检查是否有错误
                        if (data.error) {
                            statusText.textContent = '同步失败: ' + data.error;
                            statusText.classList.add('text-red-500');
                            evtSource.close();
                        }
                    } catch (e) {
                        console.error('解析进度数据失败:', e);
                    }
                };

                evtSource.onerror = function(err) {
                    console.error('SSE 连接错误:', err);
                    // 如果连接失败，显示完成按钮让用户可以手动继续
                    setTimeout(() => {
                        enterBtn.classList.remove('hidden');
                    }, 5000);
                };
            })();
        """),
        cls="sync-dialog-container",
    )


@rt("/sync-progress")
async def sync_progress(request: Request):
    """SSE 端点：提供同步进度更新"""
    global _sync_status

    async def event_generator():
        while True:
            # 发送当前状态
            data = {
                "progress": _sync_status["progress"],
                "message": _sync_status["message"],
                "completed": _sync_status["completed"],
                "error": _sync_status["error"],
            }
            yield f"data: {json.dumps(data)}\n\n"

            # 如果完成或出错，结束流
            if _sync_status["completed"] or _sync_status["error"]:
                break

            # 等待一段时间后再次发送
            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


@rt("/reset")
async def reset_initialization():
    """重置初始化状态（调试用）"""
    init_wizard.reset_initialization()
    return RedirectResponse("/init-wizard")
