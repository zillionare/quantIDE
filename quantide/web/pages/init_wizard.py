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


# ========== 步骤指示器组件 ==========

# 主色调配置
PRIMARY_COLOR = "#D13527"


def StepIndicator(current_step: int, steps: list[dict]):
    """步骤指示器组件（竖状布局）

    使用自定义样式的竖状步骤条，主色调为 #D13527。

    Args:
        current_step: 当前步骤
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
    """步骤1：欢迎页"""
    return Div(
        H3("欢迎使用 Quantide 初始化向导", cls="mb-6", style=f"color: {PRIMARY_COLOR};"),
        P("该向导将帮助您完成系统初始化与能力开关配置。", cls="text-gray-600 mb-3"),
        Ul(
            Li("配置运行环境与访问地址"),
            Li("可选配置实时行情与交易网关"),
            Li("可选配置通知告警渠道"),
            Li("配置数据初始化参数并执行首次下载"),
            cls="list-disc pl-6 mb-4 text-gray-600",
        ),
        P("若跳过网关配置，系统将仅保留策略研究能力。", cls="text-gray-500 text-sm"),
        cls="py-4",
    )


def Step3_Admin(state: dict | None = None):
    """步骤3：管理员密码设置"""
    state = state or {}

    return Div(
        H4("管理员密码", cls="mb-4", style=f"color: {PRIMARY_COLOR};"),
        P("首次初始化时必须设置管理员密码。当前版本固定使用 admin 作为管理员账号。", cls="text-gray-600 mb-3"),
        Card(
            CardBody(
                LabelInput(
                    label="管理员账号",
                    value="admin",
                    disabled=True,
                    cls="mb-3",
                ),
                P("该账号用于首次登录和后续系统管理。", cls="text-xs text-gray-500 mb-4"),
                LabelInput(
                    label="管理员密码",
                    name="admin_password",
                    type="password",
                    value=state.get("admin_password", ""),
                    placeholder="至少 6 位",
                    required=True,
                    cls="mb-3",
                ),
                LabelInput(
                    label="确认密码",
                    name="admin_password_confirm",
                    type="password",
                    value=state.get("admin_password_confirm", ""),
                    placeholder="再次输入管理员密码",
                    required=True,
                ),
                P("完成初始化后，请使用 admin 和这里设置的密码登录。", cls="text-xs text-gray-500 mt-2"),
            )
        ),
        cls="max-w-3xl mx-auto",
    )


def Step2_Runtime(state: dict | None = None):
    """步骤2：运行环境配置"""
    state = state or {}
    host = str(state.get("app_host", "0.0.0.0"))

    return Div(
        H4("运行环境", cls="mb-4", style=f"color: {PRIMARY_COLOR};"),
        P("配置服务监听地址、端口和路径前缀。", cls="text-gray-600 mb-3"),
        Card(
            CardBody(
                LabelInput(
                    label="home",
                    name="app_home",
                    value=state.get("app_home", get_runtime_home()),
                    placeholder="例如：~/.quantide",
                    required=True,
                    cls="mb-3",
                ),
                P("用途：数据与运行文件存储根目录。未配置将无法正常保存本地数据。", cls="text-xs text-gray-500 mb-4"),
                Div(
                    Label("host 访问范围", cls="text-sm font-medium"),
                    Div(
                        Label(
                            Input(
                                type="radio",
                                name="app_host",
                                value="127.0.0.1",
                                checked=host == "127.0.0.1",
                                cls="uk-radio mr-2",
                            ),
                            Span("仅限本机访问"),
                            cls="flex items-center mr-6",
                        ),
                        Label(
                            Input(
                                type="radio",
                                name="app_host",
                                value="0.0.0.0",
                                checked=host != "127.0.0.1",
                                cls="uk-radio mr-2",
                            ),
                            Span("开放访问"),
                            cls="flex items-center",
                        ),
                        cls="flex items-center mt-2",
                    ),
                    cls="mb-3",
                ),
                P("用途：控制访问范围。仅本机访问时外部设备不可连接。", cls="text-xs text-gray-500 mb-4"),
                LabelInput(
                    label="port",
                    name="app_port",
                    type="number",
                    value=state.get("app_port", 8130),
                    required=True,
                    cls="mb-3",
                ),
                P("用途：服务端口。若不配置，将使用默认端口 8130。", cls="text-xs text-gray-500 mb-4"),
                LabelInput(
                    label="prefix",
                    name="app_prefix",
                    value=state.get("app_prefix", "/"),
                    placeholder="/",
                ),
                P("用途：API 路由前缀，可不填写。默认使用 /。", cls="text-xs text-gray-500 mt-2"),
            )
        ),
        cls="max-w-3xl mx-auto",
    )


def Step4_Gateway(state: dict | None = None):
    """步骤4：网关配置"""
    state = state or {}
    enabled = bool(state.get("gateway_enabled", False))

    return Div(
        H4("实时行情与交易网关", cls="mb-4", style=f"color: {PRIMARY_COLOR};"),
        Card(
            CardBody(
                Div(
                    Input(
                        type="checkbox",
                        name="gateway_enabled",
                        value="true",
                        checked=enabled,
                        onchange="(function(cb){const btn=document.getElementById('gateway-test-btn');if(!btn){return;}if(cb.checked){btn.disabled=false;btn.style.background='#D13527';btn.style.cursor='pointer';}else{btn.disabled=true;btn.style.background='#9ca3af';btn.style.cursor='not-allowed';}})(this)",
                        cls="uk-checkbox mr-2",
                    ),
                    Span("启用 gateway（用于仿真与实盘）"),
                    cls="mb-3 flex items-center",
                ),
                P("用途：连接实时行情与交易通道。若跳过该步骤，仿真与实盘功能将不可用。", cls="text-xs text-gray-500 mb-4"),
                LabelInput(
                    label="server",
                    name="gateway_server",
                    value=state.get("gateway_server", ""),
                    placeholder="例如：127.0.0.1",
                    cls="mb-3",
                ),
                LabelInput(
                    label="port",
                    name="gateway_port",
                    type="number",
                    value=state.get("gateway_port", 8000),
                    cls="mb-3",
                ),
                LabelInput(
                    label="prefix",
                    name="gateway_prefix",
                    value=state.get("gateway_base_url", "/"),
                    placeholder="/",
                    cls="mb-3",
                ),
                P(
                    "说明：此处 prefix 指 gateway 的路径前缀，默认值为 /。网关地址由 server 与 port 组合，无需填写完整 URL。",
                    cls="text-xs text-gray-500 mb-3",
                ),
                LabelInput(
                    label="api_key",
                    name="gateway_api_key",
                    value=state.get("gateway_api_key", ""),
                    placeholder="可选：网关鉴权 key",
                ),
                Div(
                    Button(
                        "测试连通性",
                        id="gateway-test-btn",
                        disabled=not enabled,
                        cls="btn px-4 py-2 rounded mt-4",
                        style=f"background: {PRIMARY_COLOR if enabled else '#9ca3af'}; color: white; border: none; cursor: {'pointer' if enabled else 'not-allowed'};",
                        hx_post="/init-wizard/gateway-test",
                        hx_target="#gateway-test-result",
                        hx_swap="innerHTML",
                        hx_include="[name]",
                    ),
                    Div(id="gateway-test-result", cls="text-sm mt-3"),
                    cls="mt-2",
                ),
            )
        ),
        cls="max-w-3xl mx-auto",
    )


def Step5_Notify(state: dict | None = None):
    """步骤5：通知告警配置"""
    state = state or {}

    return Div(
        H4("通知告警配置", cls="mb-4", style=f"color: {PRIMARY_COLOR};"),
        Card(
            CardHeader(H5("DingTalk（可选）", cls="text-lg font-semibold")),
            CardBody(
                LabelInput(
                    label="dingtalk.access_token",
                    name="notify_dingtalk_access_token",
                    value=state.get("notify_dingtalk_access_token", ""),
                    cls="mb-3",
                ),
                LabelInput(
                    label="dingtalk.secret",
                    name="notify_dingtalk_secret",
                    value=state.get("notify_dingtalk_secret", ""),
                    cls="mb-3",
                ),
                LabelInput(
                    label="dingtalk.keyword",
                    name="notify_dingtalk_keyword",
                    value=state.get("notify_dingtalk_keyword", ""),
                ),
                P("用途：推送运行告警。未配置将无法接收钉钉告警。", cls="text-xs text-gray-500 mt-2"),
            ),
            cls="mb-4",
        ),
        Card(
            CardHeader(H5("Mail（可选）", cls="text-lg font-semibold")),
            CardBody(
                LabelInput(
                    label="mail.mail_to",
                    name="notify_mail_to",
                    value=state.get("notify_mail_to", ""),
                    cls="mb-3",
                ),
                LabelInput(
                    label="mail.mail_from",
                    name="notify_mail_from",
                    value=state.get("notify_mail_from", ""),
                    cls="mb-3",
                ),
                LabelInput(
                    label="mail.mail_server",
                    name="notify_mail_server",
                    value=state.get("notify_mail_server", ""),
                ),
                P("用途：邮件告警通道。未配置将无法接收邮件告警。", cls="text-xs text-gray-500 mt-2"),
            ),
        ),
        cls="max-w-3xl mx-auto",
    )


def Step6_DataSetup(state: dict | None = None):
    """步骤6：数据初始化与下载"""
    state = state or {}
    epoch = _format_date_zh(state.get("epoch", "2005-01-01"))
    years = state.get("history_years", 3)
    start = state.get("history_start_date", "")
    if isinstance(start, datetime.date):
        start = start.strftime("%Y-%m-%d")

    return Div(
        H4("数据初始化与下载", cls="mb-4", style=f"color: {PRIMARY_COLOR};"),
        P("本步骤将同时配置 epoch、Tushare Token、首次下载年数，并触发首次下载。", cls="text-gray-600 mb-4"),
        Card(
            CardBody(
                Div(
                    FormLabel("epoch", required=True),
                    Input(
                        type="text",
                        name="epoch",
                        value=epoch,
                        placeholder="例如：2005年01月01日",
                        cls="uk-input",
                    ),
                    P("用途：历史数据抓取起点。设置过晚会导致策略可用历史不足。", cls="text-xs text-gray-500 mt-1"),
                    cls="mb-4",
                ),
                LabelInput(
                    label="tushare token",
                    name="tushare_token",
                    value=state.get("tushare_token", ""),
                    placeholder="请输入 Tushare Token",
                    required=True,
                    cls="mb-3",
                ),
                P("用途：下载证券日历与行情数据。未配置将无法进行回测与历史数据初始化。", cls="text-xs text-gray-500 mb-4"),
                LabelInput(
                    label="首次下载长度（年）",
                    name="history_years",
                    type="number",
                    min="1",
                    value=years,
                    required=True,
                ),
                P("用途：决定首次下载范围。过小会影响策略研究样本。", cls="text-xs text-gray-500 mt-2"),
            ),
            cls="mb-4",
        ),
        P(f"将执行首次数据下载，预计范围约 {years} 年，起始日期 {start}。", cls="text-gray-600 mb-4"),
        Card(
            CardBody(
                Ul(
                    Li("证券日历"),
                    Li("全 A 证券列表"),
                    Li("历史日线行情（含复权因子与涨跌停价格）"),
                    Li("ST 数据"),
                    cls="list-disc pl-6 text-sm text-gray-600",
                ),
                P("可以点击开始下载，也可以直接点击下一步。两种方式都会触发下载。", cls="text-xs text-gray-500 mt-3"),
                Div(
                    Button(
                        "开始下载",
                        type="submit",
                        cls="btn px-6 py-2 rounded mt-4",
                        style=f"background: {PRIMARY_COLOR}; color: white; border: none;",
                        hx_post="/init-wizard/download",
                        hx_target="#wizard-form-container",
                        hx_swap="innerHTML",
                        hx_include="[name]",
                    ),
                    cls="flex justify-end",
                ),
            ),
            cls="mb-4",
        ),
        cls="max-w-3xl mx-auto",
    )


def Step7_Complete(state: dict | None = None):
    """步骤7：完成页面"""
    state = state or {}
    target = "/trade" if state.get("gateway_enabled", False) else "/strategy"
    text = "实盘" if state.get("gateway_enabled", False) else "策略研究"

    return Div(
        H3("初始化完成", cls="mb-4", style=f"color: {PRIMARY_COLOR};"),
        P(f"系统已完成初始化，正在进入{text}菜单。", cls="text-gray-600 mb-4"),
        Div(
            Button(
                f"进入{text}",
                cls="btn px-6 py-2 rounded",
                style=f"background: {PRIMARY_COLOR}; color: white; border: none;",
                hx_get=target,
            )
        ),
        cls="py-4",
    )


# ========== 导航按钮组件 ==========

def WizardButtons(current_step: int, total_steps: int = 7):
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
                cls="btn px-6 py-2 rounded",
                style="background: #f3f4f6; color: #374151; border: 1px solid #d1d5db;",
                hx_post=f"/init-wizard/step/{current_step - 1}",
                hx_target="#wizard-form-container",
                hx_swap="innerHTML",
                hx_include="[name]",
            )
        )

    if current_step < total_steps:
        if current_step == 6:
            right_buttons.append(
                Button(
                    "下一步",
                    type="submit",
                    name="nav",
                    value="next",
                    cls="btn px-6 py-2 rounded",
                    style=f"background: {PRIMARY_COLOR}; color: white; border: none;",
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
                    cls="btn px-6 py-2 rounded",
                    style=f"background: {PRIMARY_COLOR}; color: white; border: none;",
                    hx_post=f"/init-wizard/step/{current_step + 1}",
                    hx_target="#wizard-form-container",
                    hx_swap="innerHTML",
                    hx_include="[name]",
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
        {"id": 5, "name": "通知告警", "completed": state_dict.get("init_step", 0) > 5},
        {"id": 6, "name": "数据初始化与下载", "completed": state_dict.get("init_step", 0) > 6},
        {"id": 7, "name": "完成", "completed": state_dict.get("init_step", 0) >= 7},
    ]

    step_content_map = {
        1: Step1_Welcome(),
        2: Step2_Runtime(state_dict),
        3: Step3_Admin(state_dict),
        4: Step4_Gateway(state_dict),
        5: Step5_Notify(state_dict),
        6: Step6_DataSetup(state_dict),
        7: Step7_Complete(state_dict),
    }

    step_content = step_content_map.get(step, Step1_Welcome())

    return BaseLayout(
        Div(
            Style(
                """
                #wizard-form-container label { margin-right: 0.8em; }
                """
            ),
            Div(
                Div(
                    StepIndicator(step, steps),
                    cls="w-48 flex-shrink-0",
                ),
                Div(
                    Form(
                        Input(type="hidden", name="_current_step", value=str(step)),
                        Div(step_content, id="wizard-content"),
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
            init_wizard.save_gateway_config(
                enabled="gateway_enabled" in form_dict,
                server=str(form_dict.get("gateway_server", "")).strip(),
                port=int(form_dict.get("gateway_port", 8000)),
                prefix=str(form_dict.get("gateway_prefix", "/")).strip() or "/",
                api_key=str(form_dict.get("gateway_api_key", "")).strip(),
            )
        elif current_step == 5:
            init_wizard.save_notify_config(
                dingtalk_access_token=str(
                    form_dict.get("notify_dingtalk_access_token", "")
                ).strip(),
                dingtalk_secret=str(form_dict.get("notify_dingtalk_secret", "")).strip(),
                dingtalk_keyword=str(
                    form_dict.get("notify_dingtalk_keyword", "")
                ).strip(),
                mail_to=str(form_dict.get("notify_mail_to", "")).strip(),
                mail_from=str(form_dict.get("notify_mail_from", "")).strip(),
                mail_server=str(form_dict.get("notify_mail_server", "")).strip(),
            )
        elif current_step == 6:
            epoch_str = str(form_dict.get("epoch", "")).strip()
            try:
                epoch = _parse_epoch_input(epoch_str)
            except ValueError as e:
                state = init_wizard.get_state(force_refresh=True)
                step_content = Step6_DataSetup(state.to_dict())
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
                history_years=int(form_dict.get("history_years", 3)),
            )

    init_wizard.update_step(step)
    state = init_wizard.get_state(force_refresh=True)
    state_dict = state.to_dict()

    step_content_map = {
        1: Step1_Welcome(),
        2: Step2_Runtime(state_dict),
        3: Step3_Admin(state_dict),
        4: Step4_Gateway(state_dict),
        5: Step5_Notify(state_dict),
        6: Step6_DataSetup(state_dict),
        7: Step7_Complete(state_dict),
    }

    step_content = step_content_map.get(step, Step1_Welcome())
    return Div(
        Form(
            Input(type="hidden", name="_current_step", value=str(step)),
            Div(step_content, id="wizard-content"),
            WizardButtons(step),
            cls="flex-1",
        ),
        id="wizard-form-container",
        cls="flex-1 pl-8",
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
        home = state.app_home or str(getattr(cfg, "home", ""))
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
            step_content = Step6_DataSetup(state_dict)
            return Div(
                Form(
                    Input(type="hidden", name="_current_step", value="6"),
                    Div(step_content, id="wizard-content"),
                    _render_inline_error(f"下载前参数校验失败：{e}"),
                    Div(cls="mt-8"),
                    cls="flex-1",
                ),
                id="wizard-form-container",
                cls="flex-1 pl-8",
            )

    init_wizard.update_step(6)
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
    step_content = Step6_DataSetup(state_dict)
    return Div(
        Form(
            Input(type="hidden", name="_current_step", value="6"),
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
                # 完成按钮（初始隐藏）
                Button(
                    "进入系统",
                    id="complete-btn",
                    disabled=True,
                    cls="btn px-6 py-2 rounded",
                    style="background: #9ca3af; color: white; border: none; cursor: not-allowed;",
                    hx_post="/init-wizard/complete",
                    hx_target="#wizard-form-container",
                    hx_swap="innerHTML",
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
                const completeBtn = document.getElementById('complete-btn');

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
                            completeBtn.disabled = false;
                            completeBtn.style.background = '#D13527';
                            completeBtn.style.cursor = 'pointer';
                            evtSource.close();
                        }

                        // 检查是否有错误
                        if (data.error) {
                            statusText.textContent = '同步失败: ' + data.error;
                            statusText.classList.add('text-red-500');
                            stageText.textContent = '同步失败';
                            completeBtn.disabled = true;
                            completeBtn.style.background = '#9ca3af';
                            completeBtn.style.cursor = 'not-allowed';
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
                    completeBtn.disabled = true;
                    completeBtn.style.background = '#9ca3af';
                    completeBtn.style.cursor = 'not-allowed';
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
