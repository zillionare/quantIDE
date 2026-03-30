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

from quantide.config.paths import DEFAULT_DATA_HOME
from quantide.core.init_wizard_steps import WIZARD_TOTAL_STEPS, build_wizard_steps
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


ADMIN_FORM_FIELDS = {
    "password": "admin_password",
    "confirm": "admin_password_confirm",
}

RUNTIME_FORM_FIELDS = {
    "home": "app_home",
    "host": "app_host",
    "port": "app_port",
    "prefix": "app_prefix",
    "localhost_only": "localhost_only",
}

GATEWAY_FORM_FIELDS = {
    "enabled": "gateway_enabled",
    "server": "gateway_server",
    "port": "gateway_port",
    "api_key": "gateway_api_key",
    "prefix": "gateway_prefix",
    "state_prefix": "gateway_base_url",
}

DATA_INIT_FORM_FIELDS = {
    "epoch": "epoch",
    "tushare_token": "tushare_token",
    "history_years": "history_years",
}

RUNTIME_FIELD_ALIASES = {
    RUNTIME_FORM_FIELDS["home"]: (RUNTIME_FORM_FIELDS["home"], "home"),
    RUNTIME_FORM_FIELDS["host"]: (RUNTIME_FORM_FIELDS["host"], "host"),
    RUNTIME_FORM_FIELDS["port"]: (RUNTIME_FORM_FIELDS["port"], "port"),
    RUNTIME_FORM_FIELDS["prefix"]: (RUNTIME_FORM_FIELDS["prefix"], "prefix"),
}

GATEWAY_FIELD_ALIASES = {
    GATEWAY_FORM_FIELDS["enabled"]: (GATEWAY_FORM_FIELDS["enabled"],),
    GATEWAY_FORM_FIELDS["server"]: (GATEWAY_FORM_FIELDS["server"],),
    GATEWAY_FORM_FIELDS["port"]: (GATEWAY_FORM_FIELDS["port"],),
    GATEWAY_FORM_FIELDS["api_key"]: (GATEWAY_FORM_FIELDS["api_key"],),
    GATEWAY_FORM_FIELDS["prefix"]: (
        GATEWAY_FORM_FIELDS["prefix"],
        GATEWAY_FORM_FIELDS["state_prefix"],
    ),
}

DATA_INIT_FIELD_ALIASES = {
    DATA_INIT_FORM_FIELDS["epoch"]: (DATA_INIT_FORM_FIELDS["epoch"],),
    DATA_INIT_FORM_FIELDS["tushare_token"]: (DATA_INIT_FORM_FIELDS["tushare_token"],),
    DATA_INIT_FORM_FIELDS["history_years"]: (DATA_INIT_FORM_FIELDS["history_years"],),
}

RUNTIME_DEFAULTS = {
    RUNTIME_FORM_FIELDS["home"]: DEFAULT_DATA_HOME,
    RUNTIME_FORM_FIELDS["host"]: "127.0.0.1",
    RUNTIME_FORM_FIELDS["port"]: 8130,
    RUNTIME_FORM_FIELDS["prefix"]: "/quantide",
}

GATEWAY_DEFAULTS = {
    GATEWAY_FORM_FIELDS["enabled"]: True,
    GATEWAY_FORM_FIELDS["server"]: "localhost",
    GATEWAY_FORM_FIELDS["port"]: 8000,
    GATEWAY_FORM_FIELDS["api_key"]: "",
    GATEWAY_FORM_FIELDS["prefix"]: "/",
}

DATA_INIT_DEFAULTS = {
    DATA_INIT_FORM_FIELDS["epoch"]: "2005-01-01",
    DATA_INIT_FORM_FIELDS["tushare_token"]: "",
    DATA_INIT_FORM_FIELDS["history_years"]: 1,
}


def _pick_first_value(
    source: dict[str, Any] | None,
    keys: tuple[str, ...],
    default: Any,
) -> Any:
    if source is None:
        return default
    for key in keys:
        if key in source:
            return source[key]
    return default


def _coerce_checkbox(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if not text:
        return default
    return text not in {"0", "false", "off", "no"}


def _normalize_form_values(
    source: dict[str, Any] | None,
    aliases: dict[str, tuple[str, ...]],
    defaults: dict[str, Any],
) -> dict[str, Any]:
    return {
        field: _pick_first_value(source, field_aliases, defaults[field])
        for field, field_aliases in aliases.items()
    }


def _extract_form_updates(
    source: dict[str, Any] | None,
    aliases: dict[str, tuple[str, ...]],
) -> dict[str, Any]:
    if source is None:
        return {}
    updates: dict[str, Any] = {}
    for field, field_aliases in aliases.items():
        for key in field_aliases:
            if key in source:
                updates[field] = source[key]
                break
    return updates


def _runtime_form_state(source: dict[str, Any] | None = None) -> dict[str, Any]:
    values = _normalize_form_values(source, RUNTIME_FIELD_ALIASES, RUNTIME_DEFAULTS)
    host = str(values[RUNTIME_FORM_FIELDS["host"]] or RUNTIME_DEFAULTS[RUNTIME_FORM_FIELDS["host"]]).strip()
    values[RUNTIME_FORM_FIELDS["host"]] = host or RUNTIME_DEFAULTS[RUNTIME_FORM_FIELDS["host"]]
    values[RUNTIME_FORM_FIELDS["localhost_only"]] = values[RUNTIME_FORM_FIELDS["host"]] == "127.0.0.1"
    return values


def _gateway_form_state(source: dict[str, Any] | None = None) -> dict[str, Any]:
    values = _normalize_form_values(source, GATEWAY_FIELD_ALIASES, GATEWAY_DEFAULTS)
    values[GATEWAY_FORM_FIELDS["enabled"]] = _coerce_checkbox(
        values[GATEWAY_FORM_FIELDS["enabled"]],
        default=True,
    )
    return values


def _data_init_form_state(source: dict[str, Any] | None = None) -> dict[str, Any]:
    return _normalize_form_values(source, DATA_INIT_FIELD_ALIASES, DATA_INIT_DEFAULTS)


def _merge_state(base_state: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    return base_state | updates


def _parse_int_input(value: Any, label: str, default: int) -> int:
    text = str(value).strip()
    if not text:
        return default
    try:
        return int(text)
    except ValueError as exc:
        raise ValueError(f"{label}必须是整数") from exc


def _parse_positive_int_input(value: Any, label: str, default: int) -> int:
    parsed = _parse_int_input(value, label, default)
    return max(1, parsed)


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


def FormRow(label: str, input_component: Any, required: bool = False, tooltip: str | None = None):
    """横向排列的表单项（label 和 input 在同一行）

    Args:
        label: 标签文本
        input_component: 输入组件
        required: 是否为必填项
        tooltip: 可选的提示文本
    """
    label_children = [Span(label, style=FONT_STYLES["label"])]
    if required:
        label_children.append(RequiredMark())
    if tooltip:
        label_children.append(InfoTooltip(tooltip))

    return Div(
        Div(*label_children, cls="flex items-center gap-1", style="min-width: 100px; flex-shrink: 0;"),
        Div(input_component, cls="flex-1"),
        cls="flex items-center gap-4 mb-4",
    )


def FormHint(text: str):
    """表单提示文本"""
    return P(text, style=FONT_STYLES["hint"], cls="mt-1 mb-3")


def SectionTitle(text: str):
    """章节标题"""
    return H3(text, cls="mb-4", style=f"{FONT_STYLES['title']} color: {PRIMARY_COLOR};")


def SectionDescription(text: str):
    """章节描述"""
    return P(text, style=FONT_STYLES["description"], cls="mb-4")


WIZARD_STEP_META = {
    1: {
        "title": "欢迎使用 Quant IDE!",
        "description": "QuantIDE 是为量化人打造的集成开发环境 -- 数据、研究、回测、实盘。",
    },
    2: {
        "title": "运行环境",
        "description": "配置行情数据存储位置、访问控制、监听端口和路径前缀。配置数据库固定保存在系统配置目录。",
    },
    3: {
        "title": "设置管理员密码",
        "description": "首次初始化时必须设置管理员密码。当前版本固定使用 admin 作为管理员账号。",
    },
    4: {
        "title": "配置交易/实时行情网关",
        "description": "配置 gateway 连接信息，用于获取实时行情和执行交易。",
    },
    5: {
        "title": "数据源设置及下载",
        "description": "配置数据源，触发首次下载。首次下载可以仅下载少量数据，后续系统会以后台任务继续下载，直到数据补齐到您设定的数据起始日。将下载以下数据：证券日历、全A证券列表、历史日线行情（含复权因子与涨跌停价格）、ST数据。",
    },
    6: {
        "title": "初始化完成",
        "description": "恭喜！您的系统已经初始化完成。点击下方按钮，立即进入系统。",
    },
}


def _get_step_meta(step: int) -> dict[str, str]:
    return WIZARD_STEP_META.get(step, WIZARD_STEP_META[1])


# ========== 步骤指示器组件 ==========


def StepIndicator(current_step: int, steps: list[dict]):
    """步骤指示器组件（垂直时间线样式）

    完全自定义实现，不使用 FastHTML 的 steps 组件。
    特点：
    - 当前步骤圆圈与右侧标题顶部对齐
    - 连接线在圆圈下方（从圆圈底部到下一个圆圈顶部）
    - 所有步骤固定间距

    Args:
        current_step: 当前步骤
        steps: 步骤列表，每个步骤包含 id, name, completed
    """
    total_steps = len(steps)

    # 布局参数
    circle_size = 32  # 圆圈大小
    step_spacing = 70  # 步骤之间的间距（圆心到圆心）
    first_step_top = 0  # 第一个步骤距离顶部的距离，与右侧 header 顶部对齐

    # 构建所有步骤元素
    step_elements = []

    for i, step in enumerate(steps, 1):
        is_active = i == current_step
        is_completed = step.get("completed", False)

        # 计算该步骤圆圈的顶部位置
        circle_top = first_step_top + (i - 1) * step_spacing

        # 圆圈样式
        if is_active:
            circle_bg = PRIMARY_COLOR
            circle_color = "white"
            circle_border = "none"
            font_weight = "600"
        elif is_completed:
            circle_bg = PRIMARY_COLOR
            circle_color = "white"
            circle_border = "none"
            font_weight = "600"
        else:
            circle_bg = "white"
            circle_color = "#9ca3af"
            circle_border = "2px solid #e5e7eb"
            font_weight = "500"

        circle_content = "✓" if is_completed else str(i)

        # 圆圈元素 - 绝对定位
        circle = Div(
            circle_content,
            style=f"""
                position: absolute;
                top: {circle_top}px;
                left: 50%;
                transform: translateX(-50%);
                width: {circle_size}px;
                height: {circle_size}px;
                border-radius: 50%;
                background: {circle_bg};
                color: {circle_color};
                border: {circle_border};
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 14px;
                font-weight: {font_weight};
                z-index: 2;
            """,
            title=step["name"],
        )
        step_elements.append(circle)

        # 连接线（在当前圆圈下方，除了最后一个）
        if i < total_steps:
            # 连接线从当前圆圈底部到下一个圆圈顶部
            line_top = circle_top + circle_size  # 当前圆圈底部
            line_height = step_spacing - circle_size  # 到下一个圆圈顶部的距离

            # 确定连接线颜色
            if is_completed:
                line_color = PRIMARY_COLOR  # 已完成步骤的连接线为红色
            else:
                line_color = "#e5e7eb"  # 未完成步骤的连接线为灰色

            line = Div(
                style=f"""
                    position: absolute;
                    left: 50%;
                    top: {line_top}px;
                    transform: translateX(-50%);
                    width: 2px;
                    height: {line_height}px;
                    background: {line_color};
                    z-index: 1;
                """
            )
            step_elements.append(line)

    # 容器高度：最后一个圆圈位置 + 圆圈大小 + 底部留白
    container_height = first_step_top + (total_steps - 1) * step_spacing + circle_size + 20

    return Div(
        *step_elements,
        cls="step-indicator-container",
        style=f"""
            position: relative;
            width: 48px;
            height: {container_height}px;
            flex-shrink: 0;
        """
    )


# ========== 步骤内容组件 ==========

def Step1_Welcome():
    """步骤1：欢迎页"""
    return Div(
        P("本向导将引导您完成以下工作：", style=FONT_STYLES["description"], cls="mb-4 mt-6"),
        Ol(
            Li("配置运行时环境，比如数据存放目录。", style=FONT_STYLES["description"]),
            Li("配置管理员密码", style=FONT_STYLES["description"]),
            Li("配置交易/实时行情网关", style=FONT_STYLES["description"]),
            Li("配置数据源并下载历史数据。", style=FONT_STYLES["description"]),
            cls="list-decimal pl-6 space-y-2",
        ),
        cls="w-full",
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
    password = state.get(ADMIN_FORM_FIELDS["password"], "")
    strength, strength_text = _check_password_strength(password) if password else ("", "")

    # 根据强度设置颜色
    strength_color = {
        "weak": "#dc2626",  # 红色
        "medium": "#ca8a04",  # 黄色
        "strong": "#16a34a",  # 绿色
        "": "#6b7280",  # 灰色（默认）
    }.get(strength, "#6b7280")

    return Div(
        Card(
            CardBody(
                # 管理员密码
                FormLabel("管理员密码", required=True, tooltip="建议使用8位以上密码，包含字母大小写、数字、特殊符号各一。"),
                Div(
                    Input(
                        name=ADMIN_FORM_FIELDS["password"],
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
                    name=ADMIN_FORM_FIELDS["confirm"],
                    type="password",
                    value=state.get(ADMIN_FORM_FIELDS["confirm"], ""),
                    placeholder="再次输入管理员密码",
                    required=True,
                    cls="uk-input mb-4",
                ),
            )
        ),
        # 密码强度检查脚本
        Script(r"""
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
        cls="w-full",
    )


def Step2_Runtime(state: dict | None = None):
    """步骤2：运行环境配置"""
    values = _runtime_form_state(state)

    return Div(
        Card(
            CardBody(
                # 数据存储位置
                FormLabel("数据存储位置", tooltip="行情数据和相关缓存将存放在此处。留空时默认使用 ~/.quantide。配置数据库固定保存在系统配置目录。"),
                Input(
                    name=RUNTIME_FORM_FIELDS["home"],
                    value=values[RUNTIME_FORM_FIELDS["home"]],
                    placeholder=DEFAULT_DATA_HOME,
                    cls="uk-input mb-4",
                ),
                # 只允许本机访问
                Div(
                    Label(
                        Input(
                            type="checkbox",
                            name=RUNTIME_FORM_FIELDS["localhost_only"],
                            value="true",
                            checked=values[RUNTIME_FORM_FIELDS["localhost_only"]],
                            cls="uk-checkbox mr-2",
                        ),
                        Span("只允许本机访问", style=FONT_STYLES["label"]),
                        InfoTooltip("勾选后仅允许本机访问，取消勾选则允许外部访问。"),
                        cls="flex items-center mb-4",
                    ),
                ),
                # 监听端口
                FormRow(
                    "监听端口",
                    Input(
                        name=RUNTIME_FORM_FIELDS["port"],
                        type="number",
                        value=values[RUNTIME_FORM_FIELDS["port"]],
                        placeholder="8130",
                        cls="uk-input",
                    ),
                    tooltip="除非端口已被其它应用占用，否则可使用默认值。",
                ),
                # 路径前缀
                FormRow(
                    "路径前缀",
                    Input(
                        name=RUNTIME_FORM_FIELDS["prefix"],
                        value=values[RUNTIME_FORM_FIELDS["prefix"]],
                        placeholder="/quantide",
                        cls="uk-input",
                    ),
                    tooltip="可选。如果不明白含义，可保持默认。",
                ),
            )
        ),
        cls="w-full",
    )


def Step4_Gateway(state: dict | None = None):
    """步骤4：网关配置"""
    values = _gateway_form_state(state)
    enabled = bool(values[GATEWAY_FORM_FIELDS["enabled"]])

    return Div(
        Card(
            CardBody(
                # 启用 gateway
                Div(
                    Label(
                        Input(
                            type="checkbox",
                            name=GATEWAY_FORM_FIELDS["enabled"],
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
                FormRow(
                    "服务器地址",
                    Input(
                        name=GATEWAY_FORM_FIELDS["server"],
                        value=values[GATEWAY_FORM_FIELDS["server"]],
                        placeholder="localhost",
                        disabled=not enabled,
                        cls=f"uk-input {'uk-disabled' if not enabled else ''}",
                    ),
                    tooltip="gateway 服务器的主机名或 IP 地址。",
                ),
                # gateway 端口
                FormRow(
                    "端口",
                    Input(
                        name=GATEWAY_FORM_FIELDS["port"],
                        type="number",
                        value=values[GATEWAY_FORM_FIELDS["port"]],
                        placeholder="8000",
                        disabled=not enabled,
                        cls=f"uk-input {'uk-disabled' if not enabled else ''}",
                    ),
                    tooltip="gateway 服务监听的端口号。",
                ),
                # gateway 访问密钥
                FormRow(
                    "访问密钥",
                    Input(
                        name=GATEWAY_FORM_FIELDS["api_key"],
                        value=values[GATEWAY_FORM_FIELDS["api_key"]],
                        placeholder="",
                        disabled=not enabled,
                        cls=f"uk-input {'uk-disabled' if not enabled else ''}",
                    ),
                    tooltip="可在 gateway 用户头像菜单中生成和查看密钥。",
                ),
                # 路径前缀
                FormRow(
                    "路径前缀",
                    Input(
                        name=GATEWAY_FORM_FIELDS["prefix"],
                        value=values[GATEWAY_FORM_FIELDS["prefix"]],
                        placeholder="/",
                        disabled=not enabled,
                        cls=f"uk-input {'uk-disabled' if not enabled else ''}",
                    ),
                    tooltip="默认值为 /。",
                ),
            )
        ),
        cls="w-full",
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


def _render_download_range_info(years: int) -> Any:
    """渲染下载范围信息"""
    download_start, download_end = _calculate_download_range(years)
    return P(
        f"当前设置将下载从 {download_start.strftime('%Y年%m月%d日')} 到 {download_end.strftime('%Y年%m月%d日')} 的数据。",
        style=FONT_STYLES["description"],
        cls="mb-4 mt-4",
    )


def Step5_DataSetup(state: dict | None = None):
    """步骤5：数据源设置及下载"""
    values = _data_init_form_state(state)
    epoch = values[DATA_INIT_FORM_FIELDS["epoch"]]
    years_raw = values[DATA_INIT_FORM_FIELDS["history_years"]]
    try:
        years = _parse_positive_int_input(years_raw, "首次下载时长", 1)
    except ValueError:
        years = 1

    # 计算下载起止时间
    download_start, download_end = _calculate_download_range(years)

    return Div(
        Card(
            CardBody(
                # 数据起始日
                FormRow(
                    "数据起始日",
                    Input(
                        name=DATA_INIT_FORM_FIELDS["epoch"],
                        value=epoch,
                        placeholder="2005-01-01",
                        required=True,
                        cls="uk-input",
                    ),
                    required=True,
                    tooltip="行情数据的起始日，为确保数据有效、一致，不建议配置太早的起始日。比如，tushare 的数据集中，ST/涨跌停历史数据可能会从2016年起。",
                ),
                # Tushare 访问密钥
                FormRow(
                    "Tushare 访问密钥",
                    Input(
                        name=DATA_INIT_FORM_FIELDS["tushare_token"],
                        value=values[DATA_INIT_FORM_FIELDS["tushare_token"]],
                        placeholder="请输入您的 tushare token",
                        required=True,
                        cls="uk-input",
                    ),
                    required=True,
                    tooltip="访问 tushare 需要密钥，请在 https://tushare.pro/user/token 页面获取。",
                ),
                # 首次下载时长
                FormRow(
                    "首次下载时长（年）",
                    Input(
                        type="number",
                        name=DATA_INIT_FORM_FIELDS["history_years"],
                        min="1",
                        value=years_raw,
                        required=True,
                        cls="uk-input",
                        hx_trigger="change",
                        hx_post="/init-wizard/update-download-range",
                        hx_target="#download-range-info",
                        hx_include="[name='history_years']",
                    ),
                    required=True,
                    tooltip="本次初始化时，会下载从今天起往前推若干年的数据，默认为1年。后续还会有后台任务继续下载，所以为使您快速进入系统使用，建议就设置为1年。下载一年的数据，大约需要30分钟左右，也取决于您账号的限速。",
                ),
            )
        ),
        # 下载范围描述
        Div(
            _render_download_range_info(years),
            id="download-range-info",
        ),
        cls="w-full",
    )


def Step6_Complete(state: dict | None = None):
    """步骤6：完成页面"""
    return Div(
        Div(
            "✓",
            cls="w-16 h-16 rounded-full flex items-center justify-center text-2xl font-semibold mx-auto",
            style=f"background: rgba(209, 53, 39, 0.12); color: {PRIMARY_COLOR};",
        ),
        P("配置已保存，您可以立即进入系统开始使用。", cls="text-center mt-6", style=FONT_STYLES["description"]),
        cls="w-full py-8",
    )


# ========== 导航按钮组件 ==========


def _build_step_content(step: int, state_dict: dict[str, Any]):
    step_content_builders = {
        1: Step1_Welcome,
        2: lambda: Step2_Runtime(state_dict),
        3: lambda: Step3_Admin(state_dict),
        4: lambda: Step4_Gateway(state_dict),
        5: lambda: Step5_DataSetup(state_dict),
        6: lambda: Step6_Complete(state_dict),
    }
    builder = step_content_builders.get(step, Step1_Welcome)
    return builder()


def _build_step_progress(state_dict: dict[str, Any]) -> list[dict[str, int | str | bool]]:
    current_step = int(state_dict.get("init_step", 0) or 0)
    return build_wizard_steps(current_step)


WIZARD_PANEL_STYLE = (
    # "background: white; border-radius: 12px; min-height: 560px; "
    # "padding: 36px 44px 28px; display: flex; flex-direction: column; "
    # "box-shadow: 0 12px 30px rgba(15, 23, 42, 0.08);"
)

WIZARD_STEP_WRAPPER_STYLE = (
    "width: 88px; min-height: 420px; display: flex; align-items: flex-start; "
    "justify-content: center; overflow: visible;"
)

WIZARD_BODY_INNER_STYLE = "width: 100%; margin: 0 auto;"


def _render_wizard_header(step: int, error_message: str | None = None):
    meta = _get_step_meta(step)
    children: list[Any] = [SectionTitle(meta["title"])]
    description = meta.get("description")
    if description:
        children.append(SectionDescription(description))
        children.append(Hr(cls="mb-4"))
    if error_message:
        children.append(_render_inline_error(error_message))
    return Div(*children, id="wizard-header-region", cls="wizard-region")


def _render_wizard_body(step_content: Any):
    return Div(
        Div(step_content, style=WIZARD_BODY_INNER_STYLE, cls="wizard-body-inner"),
        id="wizard-body-region",
        cls="wizard-region",
    )


def _render_wizard_footer(step: int, *, show_buttons: bool = True):
    children: list[Any] = []
    if show_buttons:
        children.append(WizardButtons(step))
    return Div(*children, id="wizard-footer-region", cls="wizard-region")


def _render_wizard_form(
    step: int,
    step_content: Any,
    *,
    current_step_value: int | None = None,
    error_message: str | None = None,
    show_buttons: bool = True,
):
    children: list[Any] = [
        Input(type="hidden", name="_current_step", value=str(current_step_value or step)),
        _render_wizard_header(step, error_message),
        _render_wizard_body(Div(step_content, id="wizard-content")),
        _render_wizard_footer(step, show_buttons=show_buttons),
    ]
    return Form(*children, cls="flex-1", id="wizard-form-container")


def _render_wizard_main_content(
    step: int,
    state_dict: dict[str, Any],
    *,
    step_content: Any | None = None,
    current_step_value: int | None = None,
    error_message: str | None = None,
    show_buttons: bool = True,
    extra_nodes: tuple[Any, ...] = (),
):
    form = _render_wizard_form(
        step,
        step_content or _build_step_content(step, state_dict),
        current_step_value=current_step_value,
        error_message=error_message,
        show_buttons=show_buttons,
    )
    return Div(
        Div(
            StepIndicator(step, _build_step_progress(state_dict)),
            cls="flex-shrink-0",
            id="step-indicator-wrapper",
            style=WIZARD_STEP_WRAPPER_STYLE,
        ),
        Div(
            form,
            cls="flex-1",
            style=WIZARD_PANEL_STYLE,
        ),
        *extra_nodes,
        cls="flex items-stretch w-full gap-12",
    )


def WizardButtons(current_step: int, total_steps: int = WIZARD_TOTAL_STEPS):
    """向导导航按钮 - 上一步在左，下一步在右"""
    if current_step >= total_steps:
        return Div(
            Button(
                "进入系统",
                cls="btn px-6 py-2.5 rounded-md font-medium text-sm",
                style=f"background: {PRIMARY_COLOR}; color: white; border: none; box-shadow: 0 1px 2px rgba(209, 53, 39, 0.3); transition: all 0.2s;",
                hx_post="/init-wizard/complete",
                hx_target="#wizard-main-container",
                hx_swap="innerHTML",
            ),
            cls="flex justify-end",
        )

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
                hx_target="#wizard-main-container",
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
                    hx_target="#wizard-main-container",
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
                    hx_target="#wizard-main-container",
                    hx_swap="innerHTML",
                    hx_include="[name]",
                )
            )

    return Div(
        Div(*left_buttons, cls="flex gap-3"),
        Div(*right_buttons, cls="flex gap-3"),
        cls="flex justify-between items-center",
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

    return BaseLayout(
        Div(
            Style(
                """
                #wizard-form-container {
                    min-height: 420px;
                    display: flex;
                    flex-direction: column;
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
                #wizard-header-region {
                    flex: 0 0 auto;
                }
                #wizard-body-region {
                    flex: 1 1 auto;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    padding: 12px 0;
                }
                #wizard-footer-region {
                    flex: 0 0 auto;
                    margin-top: auto;
                    padding-top: 16px;
                    border-top: 1px solid #e5e7eb;
                }
                #wizard-main-container {
                    display: flex;
                    align-items: flex-start;
                    gap: 40px;
                    max-width: 720px;
                    margin: 0 auto;
                    padding: 32px 40px;
                    background: #fefefe;
                    box-shadow: 0px 2px 3px rgba(0,0,0,0.2);
                }
                #step-indicator-wrapper {
                    min-height: 420px;
                    display: flex;
                    align-items: flex-start;
                }
                """
            ),
            Div(_render_wizard_main_content(step, state_dict), id="wizard-main-container"),
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
            state = init_wizard.get_state(force_refresh=True)
            state_dict = _merge_state(state.to_dict(), _extract_form_updates(form_dict, RUNTIME_FIELD_ALIASES))
            state_dict[RUNTIME_FORM_FIELDS["localhost_only"]] = (
                RUNTIME_FORM_FIELDS["localhost_only"] in form_dict
            )
            state_dict[RUNTIME_FORM_FIELDS["host"]] = (
                "127.0.0.1"
                if state_dict[RUNTIME_FORM_FIELDS["localhost_only"]]
                else "0.0.0.0"
            )
            try:
                init_wizard.save_runtime_config(
                    home=str(state_dict[RUNTIME_FORM_FIELDS["home"]]).strip(),
                    host=str(state_dict[RUNTIME_FORM_FIELDS["host"]]).strip(),
                    port=_parse_int_input(
                        state_dict[RUNTIME_FORM_FIELDS["port"]],
                        "监听端口",
                        8130,
                    ),
                    prefix=str(state_dict[RUNTIME_FORM_FIELDS["prefix"]]).strip(),
                )
            except Exception as e:
                state = init_wizard.get_state(force_refresh=True)
                state_dict = state.to_dict()
                return _render_wizard_main_content(
                    2,
                    state_dict,
                    step_content=Step2_Runtime(state_dict),
                    current_step_value=2,
                    error_message=str(e),
                )
        elif current_step == 3:
            password = str(form_dict.get(ADMIN_FORM_FIELDS["password"], "")).strip()
            confirm = str(form_dict.get(ADMIN_FORM_FIELDS["confirm"], "")).strip()
            state = init_wizard.get_state(force_refresh=True)
            state_dict = state.to_dict()
            state_dict[ADMIN_FORM_FIELDS["password"]] = password
            state_dict[ADMIN_FORM_FIELDS["confirm"]] = confirm
            if password != confirm:
                return _render_wizard_main_content(
                    3,
                    state_dict,
                    step_content=Step3_Admin(state_dict),
                    current_step_value=3,
                    error_message="两次输入的管理员密码不一致",
                )
            try:
                init_wizard.save_admin_password(password)
            except Exception as e:
                return _render_wizard_main_content(
                    3,
                    state_dict,
                    step_content=Step3_Admin(state_dict),
                    current_step_value=3,
                    error_message=str(e),
                )
        elif current_step == 4:
            state = init_wizard.get_state(force_refresh=True)
            state_dict = _merge_state(
                _gateway_form_state(state.to_dict()),
                _extract_form_updates(form_dict, GATEWAY_FIELD_ALIASES),
            )
            state_dict[GATEWAY_FORM_FIELDS["enabled"]] = (
                GATEWAY_FORM_FIELDS["enabled"] in form_dict
            )
            enabled = bool(state_dict[GATEWAY_FORM_FIELDS["enabled"]])
            server = str(state_dict[GATEWAY_FORM_FIELDS["server"]]).strip()
            prefix = str(state_dict[GATEWAY_FORM_FIELDS["prefix"]]).strip() or "/"
            api_key = str(state_dict[GATEWAY_FORM_FIELDS["api_key"]]).strip()
            try:
                port = _parse_int_input(
                    state_dict[GATEWAY_FORM_FIELDS["port"]],
                    "网关端口",
                    8000,
                )
            except ValueError as e:
                return _render_wizard_main_content(
                    4,
                    state_dict,
                    step_content=Step4_Gateway(state_dict),
                    current_step_value=4,
                    error_message=str(e),
                )

            # 如果启用 gateway，进行连通性校验
            if enabled:
                ok, msg = init_wizard.test_gateway_connection(server=server, port=port, prefix=prefix)
                if not ok:
                    state_dict[GATEWAY_FORM_FIELDS["enabled"]] = enabled
                    state_dict[GATEWAY_FORM_FIELDS["server"]] = server
                    state_dict[GATEWAY_FORM_FIELDS["port"]] = port
                    state_dict[GATEWAY_FORM_FIELDS["prefix"]] = prefix
                    state_dict[GATEWAY_FORM_FIELDS["api_key"]] = api_key
                    return _render_wizard_main_content(
                        4,
                        state_dict,
                        step_content=Step4_Gateway(state_dict),
                        current_step_value=4,
                        error_message=f"{msg}",
                    )

            init_wizard.save_gateway_config(
                enabled=enabled,
                server=server,
                port=port,
                prefix=prefix,
                api_key=api_key,
            )
        elif current_step == 5:
            state = init_wizard.get_state(force_refresh=True)
            state_dict = _merge_state(state.to_dict(), _extract_form_updates(form_dict, DATA_INIT_FIELD_ALIASES))
            epoch_str = str(state_dict[DATA_INIT_FORM_FIELDS["epoch"]]).strip()
            try:
                epoch = _parse_epoch_input(epoch_str)
                history_years = _parse_positive_int_input(
                    state_dict[DATA_INIT_FORM_FIELDS["history_years"]],
                    "首次下载时长",
                    1,
                )
            except ValueError as e:
                step_content = Step5_DataSetup(state_dict)
                return _render_wizard_main_content(
                    step,
                    state_dict,
                    step_content=step_content,
                    current_step_value=step,
                    error_message=str(e),
                )
            init_wizard.save_data_init_config(
                epoch=epoch,
                tushare_token=str(state_dict[DATA_INIT_FORM_FIELDS["tushare_token"]]).strip(),
                history_years=history_years,
            )

    init_wizard.update_step(step)
    state = init_wizard.get_state(force_refresh=True)
    state_dict = state.to_dict()
    return _render_wizard_main_content(step, state_dict, current_step_value=step)


@rt("/gateway-test")
async def gateway_test(request: Request):
    """网关连通性测试"""
    form_data = await request.form()
    form_dict = dict(form_data)
    values = _merge_state(
        _gateway_form_state({}),
        _extract_form_updates(form_dict, GATEWAY_FIELD_ALIASES),
    )
    values[GATEWAY_FORM_FIELDS["enabled"]] = (
        GATEWAY_FORM_FIELDS["enabled"] in form_dict
    )
    if not values[GATEWAY_FORM_FIELDS["enabled"]]:
        return Div(
            Span("ℹ️", cls="mr-2"),
            Span("未启用 gateway，已禁用连通性测试。"),
            cls="text-sm text-gray-500 mt-2 flex items-center",
        )
    server = str(values[GATEWAY_FORM_FIELDS["server"]]).strip()
    prefix = str(values[GATEWAY_FORM_FIELDS["prefix"]]).strip() or "/"
    try:
        port = _parse_int_input(values[GATEWAY_FORM_FIELDS["port"]], "网关端口", 8000)
    except ValueError as e:
        return Div(
            Span("❌", cls="mr-2"),
            Span(str(e)),
            cls="text-sm text-red-600 mt-2 flex items-center",
        )
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


@rt("/update-download-range")
async def handle_update_download_range(request: Request):
    """更新下载范围显示"""
    form_data = await request.form()
    years_raw = str(form_data.get(DATA_INIT_FORM_FIELDS["history_years"], "1")).strip()
    try:
        years = _parse_positive_int_input(years_raw, "首次下载时长", 1)
    except ValueError:
        years = 1
    return _render_download_range_info(years)


@rt("/download")
async def handle_download(request: Request):
    """开始下载初始化数据"""
    form_data = await request.form()
    form_dict = dict(form_data)
    state = init_wizard.get_state(force_refresh=True)
    state_dict = _merge_state(state.to_dict(), _extract_form_updates(form_dict, DATA_INIT_FIELD_ALIASES))
    epoch_str = str(state_dict[DATA_INIT_FORM_FIELDS["epoch"]]).strip()
    token_str = str(state_dict[DATA_INIT_FORM_FIELDS["tushare_token"]]).strip()
    years_raw = str(state_dict[DATA_INIT_FORM_FIELDS["history_years"]]).strip()
    if epoch_str or token_str or years_raw:
        try:
            epoch = _parse_epoch_input(epoch_str) if epoch_str else state.epoch
            years = _parse_positive_int_input(years_raw, "首次下载时长", state.history_years)
            token = token_str or state.tushare_token
            init_wizard.save_data_init_config(
                epoch=epoch,
                tushare_token=token,
                history_years=years,
            )
            state = init_wizard.get_state(force_refresh=True)
        except Exception as e:
            step_content = Step5_DataSetup(state_dict)
            return _render_wizard_main_content(
                5,
                state_dict,
                step_content=step_content,
                current_step_value=5,
                error_message=f"下载前参数校验失败：{e}",
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
    return _render_wizard_main_content(
        5,
        state_dict,
        step_content=Step5_DataSetup(state_dict),
        current_step_value=5,
        show_buttons=False,
        extra_nodes=(SyncProgressDialog(),),
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
