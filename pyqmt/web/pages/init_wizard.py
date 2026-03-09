"""应用程序初始化向导页面

使用 MonsterUI 实现多步骤初始化向导。
"""

import datetime
from typing import Any

from fasthtml.common import *
from loguru import logger
from monsterui.all import *

from pyqmt.service.init_wizard import init_wizard
from pyqmt.web.layouts.base import BaseLayout

from pyqmt.web.theme import AppTheme, PRIMARY_COLOR

init_wizard_app, rt = fast_app(hdrs=AppTheme.headers())


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
        # Tushare 配置（必需）
        Card(
            CardHeader(H5("Tushare 配置（必需）", cls="text-lg font-semibold")),
            CardBody(
                P(
                    "Tushare 是必需的数据源，用于获取股票行情、财务数据等。",
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
        # QMT 配置（可选，但影响功能）
        Card(
            CardHeader(
                H5("QMT 配置（可选）", cls="text-lg font-semibold"),
            ),
            CardBody(
                Alert(
                    "⚠️ 重要提示",
                    "如果不配置 QMT，实盘交易和仿真交易功能将被禁用，仅回测功能可用。",
                    cls="mb-4"
                ),
                P(
                    "QMT 是实盘/仿真交易执行端。配置后可使用实时行情和交易功能。",
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
                    placeholder="请输入 QMT 账号 ID（不配置则留空）",
                ),
                LabelInput(
                    label="QMT 安装路径",
                    name="qmt_path",
                    value=state.get("qmt_path", ""),
                    placeholder="例如: C:/国金证券QMT交易端（不配置则留空）",
                ),
            ),
            cls="mb-4 border-warning",
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


def Step5_Complete(state: dict | None = None):
    """步骤5：完成页面"""
    state = state or {}
    has_qmt = bool(state.get("qmt_account_id")) and bool(state.get("qmt_path"))

    # 功能状态列表
    features = []

    # 回测功能（始终可用）
    features.append(
        Div(
            Span("✅", cls="mr-2"),
            Span("回测功能", cls="font-medium"),
            Span(" - 使用历史数据进行策略回测", cls="text-gray-500 text-sm ml-2"),
            cls="flex items-center py-2"
        )
    )

    # 仿真/实盘交易（需要 QMT）
    if has_qmt:
        features.append(
            Div(
                Span("✅", cls="mr-2"),
                Span("仿真交易", cls="font-medium"),
                Span(" - 使用 QMT 进行仿真交易", cls="text-gray-500 text-sm ml-2"),
                cls="flex items-center py-2"
            )
        )
        features.append(
            Div(
                Span("✅", cls="mr-2"),
                Span("实盘交易", cls="font-medium"),
                Span(" - 使用 QMT 进行实盘交易", cls="text-gray-500 text-sm ml-2"),
                cls="flex items-center py-2"
            )
        )
    else:
        features.append(
            Div(
                Span("🔒", cls="mr-2"),
                Span("仿真交易", cls="font-medium text-gray-400"),
                Span(" - 未配置 QMT，已禁用", cls="text-orange-500 text-sm ml-2"),
                cls="flex items-center py-2"
            )
        )
        features.append(
            Div(
                Span("🔒", cls="mr-2"),
                Span("实盘交易", cls="font-medium text-gray-400"),
                Span(" - 未配置 QMT，已禁用", cls="text-orange-500 text-sm ml-2"),
                cls="flex items-center py-2"
            )
        )

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
        # 功能状态卡片
        Card(
            CardHeader(H5("可用功能", cls="font-semibold")),
            CardBody(
                *features,
                Div(
                    "💡 提示：如需使用仿真/实盘交易功能，可在设置中配置 QMT 账号。",
                    cls="text-sm text-gray-500 mt-4 pt-4 border-t"
                ) if not has_qmt else "",
            ),
            cls="mb-6"
        ),
        P(
            "系统将自动开始执行数据同步任务。",
            cls="text-gray-600 mb-4",
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
                hx_target="#wizard-content",
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
                hx_target="#wizard-content",
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
        5: Step5_Complete(state_dict),
    }
    step_content = step_content_map.get(step, Step1_Welcome())

    page_content = Div(
        # 左右布局：左侧步骤条，右侧内容
        Div(
            # 左侧步骤条
            Div(
                StepIndicator(step, steps),
                cls="w-56 flex-shrink-0",
            ),
            # 右侧内容
            Div(
                Form(
                    step_content,
                    WizardButtons(step),
                    id="wizard-form",
                ),
                cls="flex-1 ml-12",
            ),
            cls="flex min-h-[500px]",
        ),
        id="wizard-content",
        cls="container mx-auto max-w-5xl py-12",
    )

    return BaseLayout(
        page_content,
        page_title="初始化向导 - PyQMT",
    )


# ========== 路由处理 ==========

@rt("/")
async def get(request: Request):
    """初始化向导首页

    支持 force=true 参数强制重新进入向导（调试用）。
    """
    # 从 URL 查询字符串中获取 force 参数
    # 尝试多种方式获取参数
    force_param = request.query_params.get("force", "")
    force = str(force_param).lower() in ("true", "1", "yes")

    logger.info(f"访问初始化向导: path={request.url.path}, query={request.url.query}, force_param={force_param}, force={force}")

    # 检查是否已完成初始化（除非强制进入）
    is_init = init_wizard.is_initialized()
    logger.info(f"初始化状态: is_initialized={is_init}")

    if not force and is_init:
        logger.info("已初始化，重定向到首页")
        return RedirectResponse("/")

    logger.info("显示初始化向导页面")

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
    """完成初始化 - 显示同步进度对话框"""
    try:
        # 标记初始化完成
        init_wizard.complete_initialization()
        logger.info("初始化向导完成，开始数据同步")

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
        # 模拟进度脚本
        Script("""
            (function() {
                let progress = 0;
                const progressBar = document.getElementById('sync-progress-bar');
                const statusText = document.getElementById('sync-status');
                const enterBtn = document.getElementById('enter-system-btn');

                const stages = [
                    { progress: 20, text: "正在连接数据源..." },
                    { progress: 40, text: "正在同步股票列表..." },
                    { progress: 60, text: "正在同步历史行情..." },
                    { progress: 80, text: "正在初始化定时任务..." },
                    { progress: 100, text: "同步完成！" },
                ];

                let currentStage = 0;

                function updateProgress() {
                    if (currentStage < stages.length) {
                        const stage = stages[currentStage];
                        progressBar.style.width = stage.progress + '%';
                        statusText.textContent = stage.text;
                        currentStage++;

                        if (stage.progress === 100) {
                            setTimeout(() => {
                                enterBtn.classList.remove('hidden');
                            }, 500);
                        } else {
                            setTimeout(updateProgress, 800 + Math.random() * 400);
                        }
                    }
                }

                // 开始进度动画
                setTimeout(updateProgress, 300);
            })();
        """),
        cls="sync-dialog-container",
    )


@rt("/reset")
def reset():
    """重置初始化（调试用）"""
    init_wizard.reset_initialization()
    return RedirectResponse("/init-wizard")
