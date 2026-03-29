"""应用程序初始化向导页面

使用 MonsterUI 实现多步骤初始化向导。
"""

import asyncio
import datetime
import json
from typing import Any

import tushare as ts
from fasthtml.common import *
from loguru import logger
from monsterui.all import *
from starlette.responses import StreamingResponse

from quantide.config.runtime import get_runtime_home
from quantide.core.message import msg_hub
from quantide.data.models.calendar import calendar
from quantide.data.models.daily_bars import daily_bars
from quantide.data.models.stocks import stock_list
from quantide.data.services import StockSyncService
from quantide.service.init_wizard import init_wizard
from quantide.web.layouts.base import BaseLayout

from quantide.web.theme import AppTheme, PRIMARY_COLOR

init_wizard_app, rt = fast_app(hdrs=AppTheme.headers())


# ========== 全局同步状态 ==========
_sync_status = {
    "is_running": False,
    "current_task": "",
    "progress": 0,
    "stage": "",
    "message": "",
    "completed": False,
    "error": None,
}


def _update_sync_status(
    progress: int,
    stage: str,
    message: str | None = None,
    completed: bool = False,
    error: str | None = None,
):
    """更新同步状态"""
    global _sync_status
    _sync_status["progress"] = progress
    _sync_status["stage"] = stage
    _sync_status["message"] = message if message is not None else stage
    _sync_status["completed"] = completed
    _sync_status["error"] = error
    logger.info(f"同步进度: {progress}% - {stage} - {_sync_status['message']}")


def _format_date_zh(value: datetime.date | str) -> str:
    if isinstance(value, datetime.date):
        return value.strftime("%Y年%m月%d日")
    text = str(value or "").strip()
    if not text:
        return ""
    if "年" in text and "月" in text and "日" in text:
        return text
    try:
        dt = datetime.datetime.strptime(text, "%Y-%m-%d").date()
        return dt.strftime("%Y年%m月%d日")
    except Exception:
        return text


def _parse_epoch_input(value: str) -> datetime.date:
    text = str(value or "").strip()
    for fmt in ("%Y年%m月%d日", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"无效的日期格式: {value}")


def _render_inline_error(message: str):
    return Div(
        Div(
            Span("❌", cls="mr-2"),
            Span(message),
            cls="text-sm text-red-600 mt-3 flex items-center",
        ),
        id="wizard-error",
    )


# ========== 样式配置 ==========

# 主色调配置
PRIMARY_COLOR = "#D13527"

# 字体大小和 weight 层级
FONT_STYLES = {
    "title": "font-size: 20px; font-weight: 600;",
    "subtitle": "font-size: 16px; font-weight: 500;",
    "description": "font-size: 14px; font-weight: 400; color: #6b7280;",
    "label": "font-size: 13px; font-weight: 500; color: #374151;",
    "hint": "font-size: 12px; font-weight: 400; color: #9ca3af;",
    "required": "color: #dc2626; margin-left: 2px;",
}


def RequiredMark():
    """必填项标记 - 红色星号"""
    return Span("*", style=FONT_STYLES["required"])


def InfoTooltip(tooltip_text: str):
    """信息提示图标 - 鼠标悬停显示 tooltip

    Args:
        tooltip_text: 提示文本内容
    """
    return Span(
        "ⓘ",
        title=tooltip_text,
        style="color: #9ca3af; cursor: help; margin-left: 4px; font-size: 12px;",
        cls="tooltip-icon",
    )


def FormLabel(label: str, required: bool = False, tooltip: str | None = None):
    """表单标签组件

    Args:
        label: 标签文本
        required: 是否为必填项
        tooltip: 可选的提示文本
    """
    children = [Span(label, style=FONT_STYLES["label"])]
    if required:
        children.append(RequiredMark())
    if tooltip:
        children.append(InfoTooltip(tooltip))
    return Div(*children, cls="mb-1")


def FormHint(text: str):
    """表单提示文本"""
    return P(text, style=FONT_STYLES["hint"], cls="mt-1 mb-3")


def SectionTitle(text: str):
    """章节标题"""
    return H4(text, cls="mb-3", style=f"{FONT_STYLES['title']} color: {PRIMARY_COLOR};")


def SectionDescription(text: str):
    """章节描述"""
    return P(text, style=FONT_STYLES["description"], cls="mb-4")


# ========== 步骤指示器组件 ==========


def StepIndicator(current_step: int, steps: list[dict]):
    """步骤指示器组件（简洁数字圆圈样式）

    参考示例设计，只保留数字圆圈，更加简洁清爽。

    Args:
        current_step: 当前步骤
        steps: 步骤列表，每个步骤包含 id, name, completed
    """
    step_items = []
    total_steps = len(steps)

    for i, step in enumerate(steps, 1):
        is_active = i == current_step
        is_completed = step.get("completed", False)

        # 确定圆圈样式
        if is_active:
            # 当前步骤：实心主色调
            circle_style = f"background: {PRIMARY_COLOR}; color: white; width: 32px; height: 32px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 14px; font-weight: 600;"
        elif is_completed:
            # 已完成步骤：实心主色调，白色对勾
            circle_style = f"background: {PRIMARY_COLOR}; color: white; width: 32px; height: 32px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 14px; font-weight: 600;"
        else:
            # 未开始步骤：白色背景，灰色边框
            circle_style = "background: white; color: #9ca3af; width: 32px; height: 32px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 14px; font-weight: 500; border: 2px solid #e5e7eb;"

        # 圆圈内容
        circle_content = "✓" if is_completed else str(i)

        # 添加连接线（除了最后一个）
        if i < total_steps:
            connector = Div(
                style=f"width: 2px; height: 24px; background: {'#e5e7eb' if not is_completed else PRIMARY_COLOR}; margin: 4px 0 4px 15px;"
            )
        else:
            connector = None

        step_items.append(
            Li(
                Div(
                    Span(
                        circle_content,
                        style=circle_style,
                    ),
                    cls="flex justify-center",
                ),
                connector if connector else "",
                cls="step",
                title=step["name"],  # 鼠标悬停显示步骤名称
            )
        )

    return Ul(
        *step_items,
        cls="steps-vertical list-none p-0 m-0",
        style="width: 48px; flex-shrink: 0;",
    )


# ========== 步骤内容组件 ==========

def Step1_Welcome():
    """步骤1：欢迎页"""
    return Div(
        SectionTitle("欢迎使用 Quant IDE!"),
        SectionDescription("QuantIDE 是为量化人打造的集成开发环境 -- 数据、研究、回测、实盘。"),
        P("本向导将引导您完成以下工作：", style=FONT_STYLES["description"], cls="mb-4 mt-6"),
        Ol(
            Li("配置运行时环境，比如数据存放目录。", style=FONT_STYLES["description"]),
            Li("配置管理员密码", style=FONT_STYLES["description"]),
            Li("配置交易/实时行情网关", style=FONT_STYLES["description"]),
            Li("配置数据源并下载历史数据。", style=FONT_STYLES["description"]),
            cls="list-decimal pl-6 space-y-2",
        ),
    )


def _check_password_strength(password: str) -> tuple[str, str]:
    """检查密码强度

    Returns:
        tuple: (强度等级: 'weak'|'medium'|'strong', 提示文本)
    """
    if len(password) < 8:
        return ("weak", "密码长度不足8位")

    has_lower = any(c.islower() for c in password)
    has_upper = any(c.isupper() for c in password)
    has_digit = any(c.isdigit() for c in password)
    has_special = any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password)

    score = sum([has_lower, has_upper, has_digit, has_special])

    if score >= 4 and len(password) >= 10:
        return ("strong", "强密码 ✓")
    elif score >= 3:
        return ("medium", "中等强度")
    else:
        return ("weak", "弱密码：建议使用字母大小写、数字、特殊符号组合")


def Step3_Admin(state: dict | None = None):
    """步骤3：管理员密码设置"""
    state = state or {}
    password = state.get("admin_password", "")
    strength, strength_text = _check_password_strength(password) if password else ("", "")

    # 根据强度设置颜色
    strength_color = {
        "weak": "#dc2626",  # 红色
        "medium": "#ca8a04",  # 黄色
        "strong": "#16a34a",  # 绿色
        "": "#6b7280",  # 灰色（默认）
    }.get(strength, "#6b7280")

    return Div(
        SectionTitle("设置管理员密码"),
        SectionDescription("首次初始化时必须设置管理员密码。当前版本固定使用 admin 作为管理员账号。"),
        Card(
            CardBody(
                # 管理员账号（只读）
                FormLabel("管理员账号", tooltip="固定使用 admin 作为管理员账号，用于首次登录和系统管理。完成初始化后，请使用 admin 和设置的密码登录。"),
                Input(
                    value="admin",
                    disabled=True,
                    cls="uk-input mb-4",
                ),
                # 管理员密码
                FormLabel("管理员密码", required=True, tooltip="建议使用8位以上密码，包含字母大小写、数字、特殊符号各一。"),
                Div(
                    Input(
                        name="admin_password",
                        type="password",
                        value=password,
                        placeholder="请输入管理员密码",
                        required=True,
                        oninput="checkPasswordStrength(this.value)",
                        cls="uk-input",
                    ),
                    # 密码强度提示
                    Div(
                        Span(strength_text, style=f"{FONT_STYLES['hint']} color: {strength_color};"),
                        id="password-strength",
                        cls="mt-1 mb-4",
                    ) if password else Div(id="password-strength", cls="mt-1 mb-4"),
                ),
                # 确认密码
                FormLabel("确认密码", required=True),
                Input(
                    name="admin_password_confirm",
                    type="password",
                    value=state.get("admin_password_confirm", ""),
                    placeholder="再次输入管理员密码",
                    required=True,
                    cls="uk-input mb-4",
                ),
            )
        ),
        # 密码强度检查脚本
        Script("""
            function checkPasswordStrength(password) {
                const strengthDiv = document.getElementById('password-strength');
                if (!password) {
                    strengthDiv.innerHTML = '';
                    return;
                }

                let strength = 'weak';
                let text = '弱密码：建议使用字母大小写、数字、特殊符号组合';
                let color = '#dc2626';

                const hasLower = /[a-z]/.test(password);
                const hasUpper = /[A-Z]/.test(password);
                const hasDigit = /\d/.test(password);
                const hasSpecial = /[!@#$%^&*()_+\-=\[\]{}|;:,.<>?]/.test(password);
                const score = [hasLower, hasUpper, hasDigit, hasSpecial].filter(Boolean).length;

                if (password.length < 8) {
                    strength = 'weak';
                    text = '密码长度不足8位';
                    color = '#dc2626';
                } else if (score >= 4 && password.length >= 10) {
                    strength = 'strong';
                    text = '强密码 ✓';
                    color = '#16a34a';
                } else if (score >= 3) {
                    strength = 'medium';
                    text = '中等强度';
                    color = '#ca8a04';
                }

                strengthDiv.innerHTML = '<span style="color: ' + color + '">' + text + '</span>';
            }
        """),
        cls="max-w-3xl mx-auto",
    )


def Step2_Runtime(state: dict | None = None):
    """步骤2：运行环境配置"""
    state = state or {}
    # 默认勾选"只允许本机访问"，即 host 默认为 127.0.0.1
    host = str(state.get("host", "127.0.0.1"))
    localhost_only = host == "127.0.0.1"

    return Div(
        SectionTitle("运行环境"),
        SectionDescription("配置数据存储位置、访问控制、监听端口和路径前缀。"),
        Card(
            CardBody(
                # 数据存储位置
                FormLabel("数据存储位置", required=True, tooltip="行情数据、数据库将存放在此处。"),
                Input(
                    name="home",
                    value=state.get("home", "~/.quantide"),
                    placeholder="~/.quantide",
                    required=True,
                    cls="uk-input mb-4",
                ),
                # 只允许本机访问
                Div(
                    Label(
                        Input(
                            type="checkbox",
                            name="localhost_only",
                            value="true",
                            checked=localhost_only,
                            cls="uk-checkbox mr-2",
                        ),
                        Span("只允许本机访问", style=FONT_STYLES["label"]),
                        InfoTooltip("勾选后仅允许本机访问，取消勾选则允许外部访问。"),
                        cls="flex items-center mb-4",
                    ),
                ),
                # 监听端口
                FormLabel("监听端口", tooltip="除非端口已被其它应用占用，否则可使用默认值。"),
                Input(
                    name="port",
                    type="number",
                    value=state.get("port", 80),
                    placeholder="80",
                    cls="uk-input mb-4",
                ),
                # 路径前缀
                FormLabel("路径前缀", tooltip="可选。如果不明白含义，可保持默认。"),
                Input(
                    name="prefix",
                    value=state.get("prefix", "/"),
                    placeholder="/",
                    cls="uk-input mb-4",
                ),
            )
        ),
        cls="max-w-3xl mx-auto",
    )


def Step4_Gateway(state: dict | None = None):
    """步骤4：网关配置"""
    state = state or {}
    enabled = bool(state.get("gateway_enabled", True))  # 默认勾选

    return Div(
        SectionTitle("配置交易/实时行情网关"),
        SectionDescription("配置 gateway 连接信息，用于获取实时行情和执行交易。"),
        Card(
            CardBody(
                # 启用 gateway
                Div(
                    Label(
                        Input(
                            type="checkbox",
                            name="gateway_enabled",
                            value="true",
                            checked=enabled,
                            cls="uk-checkbox mr-2",
                        ),
                        Span("启用 gateway", style=FONT_STYLES["label"]),
                        InfoTooltip("请安装 quantide gateway 并配置，否则无法获得实时行情和执行交易。"),
                        cls="flex items-center mb-4",
                    ),
                ),
                # gateway 服务器地址
                FormLabel("服务器地址", tooltip="gateway 服务器的主机名或 IP 地址。"),
                Input(
                    name="gateway_server",
                    value=state.get("gateway_server", "localhost"),
                    placeholder="localhost",
                    disabled=not enabled,
                    cls=f"uk-input mb-4 {'uk-disabled' if not enabled else ''}",
                ),
                # gateway 端口
                FormLabel("端口", tooltip="gateway 服务监听的端口号。"),
                Input(
                    name="gateway_port",
                    type="number",
                    value=state.get("gateway_port", 8000),
                    placeholder="8000",
                    disabled=not enabled,
                    cls=f"uk-input mb-4 {'uk-disabled' if not enabled else ''}",
                ),
                # gateway 访问密钥
                FormLabel("访问密钥", tooltip="可在 gateway 用户头像菜单中生成和查看密钥。"),
                Input(
                    name="gateway_api_key",
                    value=state.get("gateway_api_key", ""),
                    placeholder="",
                    disabled=not enabled,
                    cls=f"uk-input mb-4 {'uk-disabled' if not enabled else ''}",
                ),
                # 路径前缀
                FormLabel("路径前缀", tooltip="默认值为 /。"),
                Input(
                    name="gateway_prefix",
                    value=state.get("gateway_prefix", "/"),
                    placeholder="/",
                    disabled=not enabled,
                    cls=f"uk-input mb-4 {'uk-disabled' if not enabled else ''}",
                ),
            )
        ),
        cls="max-w-3xl mx-auto",
    )


def _calculate_download_range(years: int) -> tuple[datetime.date, datetime.date]:
    """计算下载起止日期

    Args:
        years: 下载年数

    Returns:
        tuple: (开始日期, 结束日期)
    """
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=int(years * 365.25))
    return start_date, end_date


def Step5_DataSetup(state: dict | None = None):
    """步骤5：数据源设置及下载"""
    state = state or {}
    epoch = state.get("epoch", "2005-01-01")
    years = int(state.get("history_years", 1))

    # 计算下载起止时间
    download_start, download_end = _calculate_download_range(years)

    return Div(
        SectionTitle("数据源设置及下载"),
        SectionDescription("配置数据源，触发首次下载。首次下载可以仅下载少量数据，后续系统会以后台任务继续下载，直到数据补齐到您设定的数据起始日。"),
        Card(
            CardBody(
                # 数据起始日
                FormLabel("数据起始日", required=True, tooltip="行情数据的起始日，为确保数据有效、一致，不建议配置太早的起始日。比如，tushare 的数据集中，ST/涨跌停历史数据可能会从2016年起。"),
                Input(
                    name="epoch",
                    value=epoch,
                    placeholder="2005-01-01",
                    required=True,
                    cls="uk-input mb-4",
                ),
                # Tushare 访问密钥
                FormLabel("Tushare 访问密钥", required=True, tooltip="访问 tushare 需要密钥，请在 https://tushare.pro/user/token 页面获取。"),
                Input(
                    name="tushare_token",
                    value=state.get("tushare_token", ""),
                    placeholder="请输入您的 tushare token",
                    required=True,
                    cls="uk-input mb-4",
                ),
                # 首次下载时长
                FormLabel("首次下载时长（年）", required=True, tooltip="本次初始化时，会下载从今天起往前推若干年的数据，默认为1年。后续还会有后台任务继续下载，所以为使您快速进入系统使用，建议就设置为1年。下载一年的数据，大约需要30分钟左右，也取决于您账号的限速。"),
                Input(
                    type="number",
                    name="history_years",
                    min="1",
                    value=years,
                    required=True,
                    cls="uk-input mb-4",
                ),
            )
        ),
        # 下载范围描述
        Div(
            P(f"当前设置将下载从 {download_start.strftime('%Y年%m月%d日')} 到 {download_end.strftime('%Y年%m月%d日')} 的数据。", style=FONT_STYLES["description"], cls="mb-4 mt-4"),
            id="download-range-info",
        ),
        # 数据种类描述
        P("将下载以下数据种类：", style=FONT_STYLES["description"], cls="mb-2"),
        Ul(
            Li("证券日历", style=FONT_STYLES["hint"]),
            Li("全A 证券列表", style=FONT_STYLES["hint"]),
            Li("历史日线行情（含复权因子与涨跌停价格）", style=FONT_STYLES["hint"]),
            Li("ST 数据", style=FONT_STYLES["hint"]),
            cls="list-disc pl-6 mb-4",
        ),
        cls="max-w-3xl mx-auto",
    )


def Step6_Complete(state: dict | None = None):
    """步骤6：完成页面"""
    state = state or {}

    return Div(
        SectionTitle("初始化完成"),
        SectionDescription("恭喜！您的系统已经初始化完成。点击下方按钮，立即进入系统。"),
        Div(
            Button(
                "进入系统",
                cls="btn px-6 py-2 rounded",
                style=f"background: {PRIMARY_COLOR}; color: white; border: none;",
                hx_post="/init-wizard/complete",
                hx_target="#wizard-form-container",
                hx_swap="innerHTML",
            ),
            cls="mt-6",
        ),
        cls="max-w-3xl mx-auto py-4",
    )


# ========== 导航按钮组件 ==========

def WizardButtons(current_step: int, total_steps: int = 6):
    """向导导航按钮 - 上一步在左，下一步在右"""
    if current_step >= total_steps:
        return Div(cls="mt-8")

    left_buttons = []
    right_buttons = []

    if current_step > 1:
        left_buttons.append(
            Button(
                "上一步",
                type="submit",
                name="nav",
                value="prev",
                cls="btn px-5 py-2.5 rounded-md font-medium text-sm",
                style="background: white; color: #374151; border: 1px solid #d1d5db; transition: all 0.2s;",
                hx_post=f"/init-wizard/step/{current_step - 1}",
                hx_target="#wizard-form-container",
                hx_swap="innerHTML",
                hx_include="[name]",
            )
        )

    if current_step < total_steps:
        if current_step == 5:
            # 第5步（数据设置）点击下一步触发下载
            right_buttons.append(
                Button(
                    "下一步",
                    type="submit",
                    name="nav",
                    value="next",
                    cls="btn px-5 py-2.5 rounded-md font-medium text-sm",
                    style=f"background: {PRIMARY_COLOR}; color: white; border: none; box-shadow: 0 1px 2px rgba(209, 53, 39, 0.3); transition: all 0.2s;",
                    hx_post="/init-wizard/download",
                    hx_target="#wizard-form-container",
                    hx_swap="innerHTML",
                    hx_include="[name]",
                )
            )
        else:
            right_buttons.append(
                Button(
                    "下一步",
                    type="submit",
                    name="nav",
                    value="next",
                    cls="btn px-5 py-2.5 rounded-md font-medium text-sm",
                    style=f"background: {PRIMARY_COLOR}; color: white; border: none; box-shadow: 0 1px 2px rgba(209, 53, 39, 0.3); transition: all 0.2s;",
                    hx_post=f"/init-wizard/step/{current_step + 1}",
                    hx_target="#wizard-form-container",
                    hx_swap="innerHTML",
                    hx_include="[name]",
                )
            )

    return Div(
        Div(*left_buttons, cls="flex gap-3"),
        Div(*right_buttons, cls="flex gap-3"),
        cls="flex justify-between mt-16 pt-6",
    )


# ========== 主页面 ==========

def InitWizardPage(step: int = 1, form_data: dict | None = None):
    """初始化向导主页面

    Args:
        step: 当前步骤（1-7）
        form_data: 表单数据
    """
    if form_data:
        state_dict = form_data
    else:
        state = init_wizard.get_state()
        state_dict = state.to_dict()

    steps = [
        {"id": 1, "name": "欢迎", "completed": state_dict.get("init_step", 0) > 1},
        {"id": 2, "name": "运行环境", "completed": state_dict.get("init_step", 0) > 2},
        {"id": 3, "name": "管理员密码", "completed": state_dict.get("init_step", 0) > 3},
        {"id": 4, "name": "行情与交易网关", "completed": state_dict.get("init_step", 0) > 4},
        {"id": 5, "name": "数据源设置及下载", "completed": state_dict.get("init_step", 0) > 5},
        {"id": 6, "name": "完成", "completed": state_dict.get("init_step", 0) >= 6},
    ]

    step_content_map = {
        1: Step1_Welcome(),
        2: Step2_Runtime(state_dict),
        3: Step3_Admin(state_dict),
        4: Step4_Gateway(state_dict),
        5: Step5_DataSetup(state_dict),
        6: Step6_Complete(state_dict),
    }

    step_content = step_content_map.get(step, Step1_Welcome())

    return BaseLayout(
        Div(
            Style(
                """
                #wizard-form-container {
                    background: white;
                    border-radius: 8px;
                    padding: 40px 48px;
                    min-height: 480px;
                }
                #wizard-form-container .uk-input {
                    border: 1px solid #e5e7eb;
                    border-radius: 6px;
                    padding: 10px 14px;
                    font-size: 14px;
                    transition: border-color 0.2s, box-shadow 0.2s;
                }
                #wizard-form-container .uk-input:focus {
                    border-color: #D13527;
                    box-shadow: 0 0 0 3px rgba(209, 53, 39, 0.1);
                    outline: none;
                }
                #wizard-form-container .uk-input:disabled,
                #wizard-form-container .uk-input.uk-disabled {
                    background: #f9fafb;
                    color: #9ca3af;
                    cursor: not-allowed;
                }
                #wizard-form-container .uk-checkbox {
                    width: 18px;
                    height: 18px;
                    border: 2px solid #d1d5db;
                    border-radius: 4px;
                    cursor: pointer;
                }
                #wizard-form-container .uk-checkbox:checked {
                    background: #D13527;
                    border-color: #D13527;
                }
                """
            ),
            Div(
                Div(
                    StepIndicator(step, steps),
                    cls="flex-shrink-0",
                    style="padding-top: 48px;",
                    id="step-indicator-container",
                ),
                Div(
                    Form(
                        Input(type="hidden", name="_current_step", value=str(step)),
                        Div(step_content, id="wizard-content"),
                        WizardButtons(step),
                        cls="flex-1",
                    ),
                    id="wizard-form-container",
                    cls="flex-1 ml-12",
                    style="box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1), 0 1px 2px rgba(0, 0, 0, 0.06);",
                ),
                cls="flex",
                style="max-width: 900px; margin: 0 auto; padding: 32px 24px;",
            ),
        ),
        page_title="系统初始化 - Quantide",
    )


# ========== 路由处理 ==========


@rt("/")
async def get(request: Request):
    """初始化向导主页"""
    force = request.query_params.get("force", "false").lower() == "true"

    if not force:
        try:
            is_init = init_wizard.is_initialized()
            if is_init:
                return RedirectResponse("/")
        except RuntimeError as e:
            logger.warning(f"检查初始化状态时出错：{e}")

    try:
        init_wizard.start_initialization(reset_step=True)
    except RuntimeError as e:
        logger.warning(f"开始初始化流程时出错：{e}")

    state = init_wizard.get_state(force_refresh=True)
    return InitWizardPage(step=1, form_data=state.to_dict())


@rt("/step/{step}")
async def handle_step(request: Request, step: int):
    """处理步骤导航"""
    form_data = await request.form()
    form_dict = dict(form_data)
    nav = str(form_dict.get("nav", "next")).lower()
    current_step = int(str(form_dict.get("_current_step", step)).strip() or step)

    if nav != "prev":
        if current_step == 2:
            init_wizard.save_runtime_config(
                home=str(form_dict.get("app_home", "")).strip(),
                host=str(form_dict.get("app_host", "0.0.0.0")).strip(),
                port=int(form_dict.get("app_port", 8130)),
                prefix=str(form_dict.get("app_prefix", "/")).strip(),
            )
        elif current_step == 3:
            password = str(form_dict.get("admin_password", "")).strip()
            confirm = str(form_dict.get("admin_password_confirm", "")).strip()
            state = init_wizard.get_state(force_refresh=True)
            state_dict = state.to_dict()
            state_dict["admin_password"] = password
            state_dict["admin_password_confirm"] = confirm
            if password != confirm:
                return Div(
                    Form(
                        Input(type="hidden", name="_current_step", value="3"),
                        Div(Step3_Admin(state_dict), id="wizard-content"),
                        _render_inline_error("两次输入的管理员密码不一致"),
                        WizardButtons(3),
                        cls="flex-1",
                    ),
                    id="wizard-form-container",
                    cls="flex-1 pl-8",
                )
            try:
                init_wizard.save_admin_password(password)
            except Exception as e:
                return Div(
                    Form(
                        Input(type="hidden", name="_current_step", value="3"),
                        Div(Step3_Admin(state_dict), id="wizard-content"),
                        _render_inline_error(str(e)),
                        WizardButtons(3),
                        cls="flex-1",
                    ),
                    id="wizard-form-container",
                    cls="flex-1 pl-8",
                )
        elif current_step == 4:
            enabled = "gateway_enabled" in form_dict
            server = str(form_dict.get("gateway_server", "")).strip()
            port = int(form_dict.get("gateway_port", 8000))
            prefix = str(form_dict.get("gateway_prefix", "/")).strip() or "/"
            api_key = str(form_dict.get("gateway_api_key", "")).strip()

            # 如果启用 gateway，进行连通性校验
            if enabled:
                ok, msg = init_wizard.test_gateway_connection(server=server, port=port, prefix=prefix)
                if not ok:
                    state = init_wizard.get_state(force_refresh=True)
                    state_dict = state.to_dict()
                    state_dict["gateway_enabled"] = enabled
                    state_dict["gateway_server"] = server
                    state_dict["gateway_port"] = port
                    state_dict["gateway_prefix"] = prefix
                    state_dict["gateway_api_key"] = api_key
                    return Div(
                        Form(
                            Input(type="hidden", name="_current_step", value="4"),
                            Div(Step4_Gateway(state_dict), id="wizard-content"),
                            _render_inline_error(f"{msg}"),
                            WizardButtons(4),
                            cls="flex-1",
                        ),
                        id="wizard-form-container",
                        cls="flex-1 pl-8",
                    )

            init_wizard.save_gateway_config(
                enabled=enabled,
                server=server,
                port=port,
                prefix=prefix,
                api_key=api_key,
            )
        elif current_step == 5:
            epoch_str = str(form_dict.get("epoch", "")).strip()
            try:
                epoch = _parse_epoch_input(epoch_str)
            except ValueError as e:
                state = init_wizard.get_state(force_refresh=True)
                step_content = Step5_DataSetup(state.to_dict())
                return Div(
                    Form(
                        Input(type="hidden", name="_current_step", value=str(step)),
                        Div(step_content, id="wizard-content"),
                        _render_inline_error(str(e)),
                        WizardButtons(step),
                        cls="flex-1",
                    ),
                    id="wizard-form-container",
                    cls="flex-1 pl-8",
                )
            init_wizard.save_data_init_config(
                epoch=epoch,
                tushare_token=str(form_dict.get("tushare_token", "")).strip(),
                history_years=int(form_dict.get("history_years", 1)),
            )

    init_wizard.update_step(step)
    state = init_wizard.get_state(force_refresh=True)
    state_dict = state.to_dict()

    # 构建步骤列表
    steps = [
        {"id": 1, "name": "欢迎", "completed": state_dict.get("init_step", 0) > 1},
        {"id": 2, "name": "运行环境", "completed": state_dict.get("init_step", 0) > 2},
        {"id": 3, "name": "管理员密码", "completed": state_dict.get("init_step", 0) > 3},
        {"id": 4, "name": "行情与交易网关", "completed": state_dict.get("init_step", 0) > 4},
        {"id": 5, "name": "数据源设置及下载", "completed": state_dict.get("init_step", 0) > 5},
        {"id": 6, "name": "完成", "completed": state_dict.get("init_step", 0) >= 6},
    ]

    step_content_map = {
        1: Step1_Welcome(),
        2: Step2_Runtime(state_dict),
        3: Step3_Admin(state_dict),
        4: Step4_Gateway(state_dict),
        5: Step5_DataSetup(state_dict),
        6: Step6_Complete(state_dict),
    }

    step_content = step_content_map.get(step, Step1_Welcome())

    # 返回包含步骤指示器和表单内容的完整结构
    # 使用 hx-swap-oob 来更新步骤指示器
    return Div(
        # 步骤指示器 - 使用 hx-swap-oob 更新左侧
        Div(
            StepIndicator(step, steps),
            cls="w-48 flex-shrink-0",
            id="step-indicator-container",
            hx_swap_oob="true",
        ),
        # 表单内容
        Form(
            Input(type="hidden", name="_current_step", value=str(step)),
            Div(step_content, id="wizard-content"),
            WizardButtons(step),
            cls="flex-1",
            id="wizard-form-container",
        ),
        cls="flex",
    )


@rt("/gateway-test")
async def gateway_test(request: Request):
    """网关连通性测试"""
    form_data = await request.form()
    form_dict = dict(form_data)
    if "gateway_enabled" not in form_dict:
        return Div(
            Span("ℹ️", cls="mr-2"),
            Span("未启用 gateway，已禁用连通性测试。"),
            cls="text-sm text-gray-500 mt-2 flex items-center",
        )
    server = str(form_dict.get("gateway_server", "")).strip()
    port = int(form_dict.get("gateway_port", 8000))
    prefix = str(form_dict.get("gateway_prefix", "/")).strip() or "/"
    ok, msg = init_wizard.test_gateway_connection(server=server, port=port, prefix=prefix)
    if ok:
        return Div(
            Span("✅", cls="mr-2"),
            Span("连通性测试正确：", cls="font-semibold mr-1"),
            Span(msg),
            cls="text-sm text-green-600 mt-2 flex items-center",
        )
    return Div(
        Span("❌", cls="mr-2"),
        Span(msg),
        cls="text-sm text-red-600 mt-2 flex items-center",
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
        state = init_wizard.get_state(force_refresh=True)
        home = state.app_home
        token = state.tushare_token
        effective_start = start_date or state.history_start_date or state.epoch

        _update_sync_status(5, "正在初始化数据层", "正在初始化数据层...")
        from quantide.data import init_data

        init_data(home, init_db=True)
        ts.set_token(token)

        stock_sync = StockSyncService(stock_list, daily_bars.store, calendar)

        _update_sync_status(15, "正在同步证券日历", "正在同步证券日历...")
        await asyncio.to_thread(calendar.update)

        _update_sync_status(30, "正在同步全A证券列表", "正在同步全A证券列表...")
        stock_count = await asyncio.to_thread(stock_sync.sync_stock_list)
        _update_sync_status(
            40,
            "全A证券列表同步完成",
            f"全A证券列表同步完成，共 {stock_count} 条",
        )

        _update_sync_status(45, "正在准备同步历史行情相关数据", "正在准备同步历史行情相关数据...")

        stage_label = {
            "bars": "正在同步历史日线行情",
            "adjust": "正在同步复权因子",
            "limit": "正在同步涨跌停",
            "st": "正在同步 ST 数据",
            "done": "当日数据同步完成",
        }
        stage_offset = {
            "bars": 0.10,
            "adjust": 0.35,
            "limit": 0.60,
            "st": 0.85,
            "done": 1.00,
        }

        def _on_fetch_progress(payload):
            if not isinstance(payload, dict):
                return
            if payload.get("error"):
                _update_sync_status(
                    _sync_status["progress"],
                    "同步失败",
                    f"同步失败: {payload['error']}",
                    error=str(payload["error"]),
                )
                return
            if "msg" in payload and "completed" not in payload:
                msg = str(payload.get("msg", "")).strip()
                if msg:
                    _update_sync_status(
                        _sync_status["progress"],
                        msg,
                        _sync_status.get("message", msg),
                    )
                return
            if "completed" not in payload or "total" not in payload:
                return
            phase = str(payload.get("phase", "done"))
            completed = int(payload.get("completed", 0))
            total = int(payload.get("total", 0))
            if total <= 0 or completed <= 0:
                return
            date_str = str(payload.get("current_date", "")).strip()
            try:
                current_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                current_date_zh = current_date.strftime("%Y年%m月%d日")
            except Exception:
                current_date_zh = date_str
            if not current_date_zh:
                return
            phase_ratio = stage_offset.get(phase, 1.0)
            progress = 45 + int((((max(completed, 1) - 1) + phase_ratio) / max(total, 1)) * 50)
            msg = (
                f"正在同步 {current_date_zh}，当前进度 {completed}/{total}"
            )
            _update_sync_status(progress, stage_label.get(phase, "正在同步数据"), msg)

        msg_hub.subscribe("fetch_data_progress", _on_fetch_progress)
        try:
            await asyncio.to_thread(
                stock_sync.sync_daily_bars,
                effective_start,
                None,
            )
        finally:
            msg_hub.unsubscribe("fetch_data_progress", _on_fetch_progress)

        _update_sync_status(98, "数据下载完成", "数据下载完成，正在收尾...")
        init_wizard.complete_initialization()
        _update_sync_status(100, "初始化数据下载完成", "初始化数据下载完成", completed=True)
        logger.info("数据同步全部完成")
    except Exception as e:
        logger.error(f"数据同步过程中发生错误: {e}")
        _update_sync_status(
            _sync_status["progress"],
            "同步失败",
            f"同步失败: {e}",
            error=str(e),
        )
    finally:
        _sync_status["is_running"] = False


@rt("/download")
async def handle_download(request: Request):
    """开始下载初始化数据"""
    form_data = await request.form()
    form_dict = dict(form_data)
    state = init_wizard.get_state(force_refresh=True)
    epoch_str = str(form_dict.get("epoch", "")).strip()
    token_str = str(form_dict.get("tushare_token", "")).strip()
    years_raw = str(form_dict.get("history_years", "")).strip()
    if epoch_str or token_str or years_raw:
        try:
            epoch = _parse_epoch_input(epoch_str) if epoch_str else state.epoch
            years = int(years_raw) if years_raw else state.history_years
            token = token_str or state.tushare_token
            init_wizard.save_data_init_config(
                epoch=epoch,
                tushare_token=token,
                history_years=years,
            )
            state = init_wizard.get_state(force_refresh=True)
        except Exception as e:
            state_dict = state.to_dict()
            step_content = Step5_DataSetup(state_dict)
            return Div(
                Form(
                    Input(type="hidden", name="_current_step", value="5"),
                    Div(step_content, id="wizard-content"),
                    _render_inline_error(f"下载前参数校验失败：{e}"),
                    Div(cls="mt-8"),
                    cls="flex-1",
                ),
                id="wizard-form-container",
                cls="flex-1 pl-8",
            )

    init_wizard.update_step(5)
    state = init_wizard.get_state(force_refresh=True)

    global _sync_status
    _sync_status = {
        "is_running": True,
        "current_task": "",
        "progress": 0,
        "stage": "正在初始化",
        "message": "正在初始化...",
        "completed": False,
        "error": None,
    }
    asyncio.create_task(_run_data_sync(state.history_start_date))
    logger.info(f"开始下载历史数据，起始日期: {state.history_start_date}")

    state_dict = state.to_dict()
    step_content = Step5_DataSetup(state_dict)
    return Div(
        Form(
            Input(type="hidden", name="_current_step", value="5"),
            Div(step_content, id="wizard-content"),
            Div(cls="mt-8"),
            cls="flex-1",
        ),
        SyncProgressDialog(),
        id="wizard-form-container",
        cls="flex-1 pl-8",
    )


@rt("/complete")
async def handle_complete():
    """完成初始化并跳转目标菜单"""
    try:
        init_wizard.complete_initialization()
        target = init_wizard.get_completion_redirect()
        return Div(
            Script(f"window.location.href = '{target}'"),
            P("正在跳转...", cls="text-center p-4"),
        )
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
                P("正在同步数据...", id="sync-stage", cls="text-gray-600 mb-4"),
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
                cls="bg-white rounded-lg shadow-xl p-6 max-w-md w-full mx-4",
            ),
            cls="fixed inset-0 flex items-center justify-center z-50",
        ),
        # SSE 连接脚本
        Script("""
            (function() {
                const progressBar = document.getElementById('sync-progress-bar');
                const stageText = document.getElementById('sync-stage');
                const statusText = document.getElementById('sync-status');

                // 创建 EventSource 连接
                const evtSource = new EventSource('/init-wizard/sync-progress');

                evtSource.onmessage = function(event) {
                    try {
                        const data = JSON.parse(event.data);

                        // 更新进度条
                        progressBar.style.width = data.progress + '%';
                        statusText.textContent = data.message || '等待同步进度...';
                        stageText.textContent = data.stage || '正在同步数据...';

                        // 检查是否完成
                        if (data.completed) {
                            evtSource.close();
                            // 同步完成后自动跳转到完成页面
                            setTimeout(function() {
                                window.location.href = '/init-wizard/complete';
                            }, 1000);
                        }

                        // 检查是否有错误
                        if (data.error) {
                            statusText.textContent = '同步失败: ' + data.error;
                            statusText.classList.add('text-red-500');
                            stageText.textContent = '同步失败';
                            evtSource.close();
                        }
                    } catch (e) {
                        console.error('解析进度数据失败:', e);
                    }
                };

                evtSource.onerror = function(err) {
                    console.error('SSE 连接错误:', err);
                    statusText.textContent = '同步连接异常，请稍后重试';
                    statusText.classList.add('text-red-500');
                    stageText.textContent = '同步连接异常';
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
                "stage": _sync_status.get("stage", ""),
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
