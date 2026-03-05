"""应用程序初始化向导页面

使用 MonsterUI 实现多步骤初始化向导。
"""

import datetime
from typing import Any

from fasthtml.common import *
from loguru import logger
from monsterui.all import *
from monsterui.daisy import Steps, LiStep, StepT

from pyqmt.service.init_wizard import init_wizard
from pyqmt.web.layouts.base import BaseLayout

init_wizard_app, rt = fast_app()


# ========== 步骤指示器组件 ==========

def StepIndicator(current_step: int, steps: list[dict]):
    """步骤指示器组件

    使用 MonsterUI 的 Steps 组件实现。

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
            cls = StepT.primary.value
        elif is_completed:
            cls = StepT.success.value
        else:
            cls = ""

        step_items.append(
            LiStep(
                step["name"],
                cls=cls,
                data_content=str(i),
            )
        )

    return Steps(*step_items, cls="mb-8")


# ========== 步骤内容组件 ==========

def Step1_Welcome():
    """步骤1：欢迎页面"""
    return Div(
        H3("欢迎使用 PyQMT", cls="text-center mb-4"),
        P(
            "PyQMT 是一个基于 Python 的量化交易系统。",
            cls="text-center text-gray-600 mb-4",
        ),
        P(
            "在开始使用之前，我们需要完成一些初始化配置，包括：",
            cls="text-center text-gray-600 mb-4",
        ),
        Ul(
            Li("配置数据源（Tushare、QMT）"),
            Li("设置定时任务时间"),
            Li("下载历史行情数据"),
            cls="list-disc pl-8 mb-6 text-gray-600",
        ),
        P(
            "整个初始化过程大约需要 5-10 分钟，取决于您选择下载的历史数据范围。",
            cls="text-center text-gray-500 text-sm mb-6",
        ),
        cls="max-w-2xl mx-auto py-8",
    )


def Step2_DataSource(state: dict | None = None):
    """步骤2：数据源配置"""
    state = state or {}

    return Div(
        H4("配置数据源", cls="mb-4"),
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
        # QMT 配置
        Card(
            CardHeader(H5("QMT 配置", cls="text-lg font-semibold")),
            CardBody(
                P(
                    "QMT 是交易执行端，用于实盘或仿真交易。",
                    cls="text-sm text-gray-600 mb-4",
                ),
                # 账号类型选择
                Div(
                    FormLabel("账号类型", required=True),
                    Div(
                        LabelRadio(
                            label="仿真交易",
                            name="qmt_account_type",
                            value="simulation",
                            checked=state.get("qmt_account_type") == "simulation",
                        ),
                        LabelRadio(
                            label="实盘交易",
                            name="qmt_account_type",
                            value="live",
                            checked=state.get("qmt_account_type") == "live",
                        ),
                        cls="flex gap-4 mt-2",
                    ),
                    cls="mb-4",
                ),
                LabelInput(
                    label="QMT 账号 ID",
                    name="qmt_account_id",
                    value=state.get("qmt_account_id", ""),
                    placeholder="请输入 QMT 账号 ID",
                    required=True,
                ),
                LabelInput(
                    label="QMT 安装路径",
                    name="qmt_path",
                    value=state.get("qmt_path", ""),
                    placeholder="例如: C:/国金证券QMT交易端",
                    required=True,
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
        H4("配置任务调度时间", cls="mb-4"),
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
                    ),
                    # 晚间任务
                    Div(
                        H6("晚间任务", cls="font-semibold mb-3"),
                        LabelInput(
                            label="板块数据同步时间",
                            name="sector_sync_time",
                            type="time",
                            value=state.get("sector_sync_time", "19:00"),
                            cls="mb-3",
                        ),
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
        H4("下载历史数据", cls="mb-4"),
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
            cls="flex justify-center mb-4",
        ),
        H3("初始化完成！", cls="text-center mb-4"),
        P(
            "您的 PyQMT 系统已完成初始化配置。",
            cls="text-center text-gray-600 mb-4",
        ),
        P(
            "系统将自动开始执行数据同步任务，您现在可以：",
            cls="text-center text-gray-600 mb-4",
        ),
        Ul(
            Li("查看实时行情"),
            Li("创建和运行策略"),
            Li("进行回测或实盘交易"),
            Li("分析历史数据"),
            cls="list-disc pl-8 mb-6 text-gray-600 max-w-md mx-auto",
        ),
        cls="max-w-2xl mx-auto py-8 text-center",
    )


# ========== 导航按钮组件 ==========

def WizardButtons(current_step: int, total_steps: int = 5):
    """向导导航按钮"""
    buttons = []

    # 上一步按钮（除了第一步）
    if current_step > 1:
        buttons.append(
            Button(
                "上一步",
                cls=ButtonT.secondary,
                hx_post=f"/init-wizard/step/{current_step - 1}",
                hx_target="#wizard-content",
                hx_include="[name]",
            )
        )

    # 下一步/完成按钮
    if current_step < total_steps:
        buttons.append(
            Button(
                "下一步",
                cls=ButtonT.primary,
                hx_post=f"/init-wizard/step/{current_step + 1}",
                hx_target="#wizard-content",
                hx_include="[name]",
            )
        )
    else:
        buttons.append(
            Button(
                "进入系统",
                cls=ButtonT.primary,
                hx_post="/init-wizard/complete",
                hx_target="body",
                hx_push_url="/",
            )
        )

    return Div(*buttons, cls="flex justify-between mt-8")


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

    page_content = Div(
        H2("PyQMT 初始化向导", cls="text-center mb-2"),
        P(
            f"步骤 {step} / 5",
            cls="text-center text-gray-500 mb-6",
        ),
        # 步骤指示器
        StepIndicator(step, steps),
        # 步骤内容
        Form(
            step_content,
            WizardButtons(step),
            id="wizard-form",
        ),
        id="wizard-content",
        cls="container mx-auto max-w-4xl py-8",
    )

    return BaseLayout(
        page_content,
        page_title="初始化向导 - PyQMT",
    )


# ========== 路由处理 ==========

@rt("/")
def get():
    """初始化向导首页"""
    # 检查是否已完成初始化
    if init_wizard.is_initialized():
        return RedirectResponse("/")

    # 开始初始化流程
    init_wizard.start_initialization()

    return InitWizardPage(step=1)


@rt("/step/{step}")
async def handle_step(request: Request, step: int):
    """处理步骤切换"""
    # 获取表单数据
    form_data = dict(await request.form())

    # 保存当前步骤的数据
    current_step = step - 1

    try:
        if current_step == 2:
            # 保存数据源配置
            init_wizard.save_data_source_config(
                tushare_token=str(form_data.get("tushare_token", "")),
                qmt_account_id=str(form_data.get("qmt_account_id", "")),
                qmt_account_type=str(form_data.get("qmt_account_type", "simulation")),
                qmt_path=str(form_data.get("qmt_path", "")),
            )
        elif current_step == 3:
            # 保存任务调度配置
            init_wizard.save_schedule_config(
                daily_fetch_time=str(form_data.get("daily_fetch_time", "16:00")),
                limit_refresh_time=str(form_data.get("limit_refresh_time", "09:00")),
                adj_factor_time=str(form_data.get("adj_factor_time", "09:20")),
                sector_sync_time=str(form_data.get("sector_sync_time", "19:00")),
                index_sync_time=str(form_data.get("index_sync_time", "19:30")),
            )
        elif current_step == 4:
            # 保存历史数据配置
            start_date_str = form_data.get("history_start_date", "")
            if start_date_str:
                start_date = datetime.datetime.strptime(
                    str(start_date_str), "%Y-%m-%d"
                ).date()
                init_wizard.save_history_config(start_date)

        # 更新步骤
        init_wizard.update_step(step)

    except Exception as e:
        logger.error(f"保存配置失败: {e}")
        # 可以在这里添加错误提示

    return InitWizardPage(step=step, form_data=form_data)


@rt("/complete")
async def handle_complete():
    """完成初始化"""
    try:
        init_wizard.complete_initialization()
        logger.info("初始化向导完成，即将进入主界面")
        return RedirectResponse("/", status_code=303)
    except Exception as e:
        logger.error(f"完成初始化失败: {e}")
        return Div(
            P(f"初始化失败: {e}", cls="text-red-500"),
            cls="text-center p-4",
        )


@rt("/reset")
def reset():
    """重置初始化（调试用）"""
    init_wizard.reset_initialization()
    return RedirectResponse("/init-wizard")
