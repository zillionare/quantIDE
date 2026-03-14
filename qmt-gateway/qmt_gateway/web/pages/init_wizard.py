"""初始化向导页面

4 步初始化向导：
1. 欢迎页面
2. 管理员设置
3. 服务器设置
4. QMT 配置
"""

from fasthtml.common import *
from monsterui.all import *

from qmt_gateway.web.layouts.base import create_base_page
from qmt_gateway.web.theme import PRIMARY_COLOR, PrimaryButton, SecondaryButton


def StepIndicator(current_step: int, total_steps: int = 4):
    """步骤指示器"""
    steps = [
        ("欢迎", 1),
        ("管理员", 2),
        ("服务器", 3),
        ("QMT配置", 4),
    ]

    items = []
    for name, step in steps:
        is_active = step == current_step
        is_completed = step < current_step

        if is_active:
            circle_style = f"background: {PRIMARY_COLOR}; color: white;"
            text_style = f"color: {PRIMARY_COLOR}; font-weight: bold;"
        elif is_completed:
            circle_style = f"background: {PRIMARY_COLOR}; color: white;"
            text_style = f"color: {PRIMARY_COLOR};"
        else:
            circle_style = "background: #e5e7eb; color: #9ca3af;"
            text_style = "color: #9ca3af;"

        items.append(
            Div(
                Div(
                    str(step),
                    cls="w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold",
                    style=circle_style,
                ),
                Div(name, cls="text-xs mt-1", style=text_style),
                cls="flex flex-col items-center mx-2",
            )
        )

    return Div(*items, cls="flex justify-center mb-8")


def Step1_Welcome():
    """步骤1：欢迎页面"""
    return Div(
        H4("欢迎使用 QMT Gateway", cls="text-xl font-semibold mb-4", style=f"color: {PRIMARY_COLOR};"),
        P("QMT Gateway 是一个基于 Python 的量化交易网关。", cls="text-gray-600 mb-4"),
        P("在开始使用之前，我们需要完成一些初始化配置：", cls="text-gray-600 mb-4"),
        Ul(
            Li("设置管理员账号"),
            Li("配置服务器和日志"),
            Li("配置 QMT 账号和路径"),
            cls="list-disc list-inside text-gray-600 mb-4",
        ),
        P("整个初始化过程大约需要 1 分钟。", cls="text-gray-500 text-sm"),
        cls="max-w-lg mx-auto",
    )


def Step2_Admin():
    """步骤2：管理员设置"""
    return Div(
        H4("设置管理员账号", cls="text-xl font-semibold mb-4", style=f"color: {PRIMARY_COLOR};"),
        Card(
            CardBody(
                Div(
                    Label("用户名", cls="label"),
                    Input(
                        type="text",
                        name="username",
                        value="admin",
                        cls="input input-bordered w-full",
                        required=True,
                    ),
                    cls="mb-4",
                ),
                Div(
                    Label("密码", cls="label"),
                    Input(
                        type="password",
                        name="password",
                        placeholder="请输入密码",
                        cls="input input-bordered w-full",
                        required=True,
                    ),
                    cls="mb-4",
                ),
                Div(
                    Label("确认密码", cls="label"),
                    Input(
                        type="password",
                        name="password_confirm",
                        placeholder="请再次输入密码",
                        cls="input input-bordered w-full",
                        required=True,
                    ),
                    cls="mb-4",
                ),
            ),
            cls="mb-4",
        ),
        cls="max-w-lg mx-auto",
    )


def Step3_Server():
    """步骤3：服务器设置"""
    return Div(
        H4("服务器设置", cls="text-xl font-semibold mb-4", style=f"color: {PRIMARY_COLOR};"),
        P("建议使用默认配置。", cls="text-gray-600 mb-4"),
        Card(
            CardBody(
                Div(
                    Label("服务器端口", cls="label"),
                    Input(
                        type="number",
                        name="server_port",
                        value="8130",
                        cls="input input-bordered w-full",
                        required=True,
                    ),
                    cls="mb-4",
                ),
                Div(
                    Label("日志路径", cls="label"),
                    Input(
                        type="text",
                        name="log_path",
                        value="~/.qmt-gateway/log",
                        cls="input input-bordered w-full",
                        required=True,
                    ),
                    cls="mb-4",
                ),
                Div(
                    Label("日志轮转大小", cls="label"),
                    Input(
                        type="text",
                        name="log_rotation",
                        value="10 MB",
                        cls="input input-bordered w-full",
                        required=True,
                    ),
                    cls="mb-4",
                ),
                Div(
                    Label("日志保留数量", cls="label"),
                    Input(
                        type="number",
                        name="log_retention",
                        value="10",
                        cls="input input-bordered w-full",
                        required=True,
                    ),
                    cls="mb-4",
                ),
            ),
            cls="mb-4",
        ),
        cls="max-w-lg mx-auto",
    )


def Step4_QMT():
    """步骤4：QMT 配置"""
    return Div(
        H4("QMT 配置", cls="text-xl font-semibold mb-4", style=f"color: {PRIMARY_COLOR};"),
        Card(
            CardBody(
                Div(
                    Label("QMT 账号", cls="label"),
                    Input(
                        type="text",
                        name="qmt_account_id",
                        placeholder="请输入 QMT 账号",
                        cls="input input-bordered w-full",
                        required=True,
                    ),
                    cls="mb-4",
                ),
                Div(
                    Label("QMT 路径", cls="label"),
                    Input(
                        type="text",
                        name="qmt_path",
                        placeholder=r"例如: C:\国金证券QMT交易端\userdata_mini",
                        cls="input input-bordered w-full",
                        required=True,
                    ),
                    P("提示：输入包含 userdata_mini 的完整路径。如果不知道安装位置，可以在文件资源管理器中搜索 userdata_mini", cls="text-xs text-gray-500 mt-1"),
                    cls="mb-4",
                ),
                Div(
                    Label("xtquant 路径", cls="label"),
                    Input(
                        type="text",
                        name="xtquant_path",
                        placeholder=r"例如: C:\apps",
                        cls="input input-bordered w-full",
                    ),
                    P(r"提示：输入 xtquant 解压后的父目录路径。如果解压到 C:\apps\xtquant，则填入 C:\apps", cls="text-xs text-gray-500 mt-1"),
                    cls="mb-4",
                ),
            ),
            cls="mb-4",
        ),
        cls="max-w-lg mx-auto",
    )


def WizardContent(step: int, form_data: dict | None = None):
    """根据步骤渲染内容"""
    content_map = {
        1: Step1_Welcome(),
        2: Step2_Admin(),
        3: Step3_Server(),
        4: Step4_QMT(),
    }
    return content_map.get(step, Step1_Welcome())


def WizardButtons(step: int, total_steps: int = 4):
    """向导导航按钮"""
    left_buttons = []
    right_buttons = []

    # 上一步按钮
    if step > 1:
        left_buttons.append(
            SecondaryButton(
                "上一步",
                hx_post=f"/init-wizard/step/{step - 1}",
                hx_target="#wizard-form-container",
                hx_include="#wizard-form",
            )
        )

    # 下一步/完成按钮
    if step < total_steps:
        right_buttons.append(
            PrimaryButton(
                "下一步",
                hx_post=f"/init-wizard/step/{step + 1}",
                hx_target="#wizard-form-container",
                hx_include="#wizard-form",
            )
        )
    else:
        # 最后一步：完成初始化
        right_buttons.append(
            PrimaryButton(
                "完成初始化",
                hx_post="/init-wizard/complete",
                hx_target="#wizard-form-container",
                hx_include="#wizard-form",
            )
        )

    return Div(
        Div(*left_buttons, cls="flex gap-2"),
        Div(*right_buttons, cls="flex gap-2"),
        cls="flex justify-between mt-6 pt-4 border-t",
    )


def InitWizardForm(step: int = 1, form_data: dict | None = None, error: str = None):
    """初始化向导表单（用于 HTMX 更新，包含步骤指示器和表单内容）

    Args:
        step: 当前步骤
        form_data: 表单数据
        error: 错误信息（如果有）
    """
    content = WizardContent(step, form_data)
    buttons = WizardButtons(step)

    # 错误提示
    error_div = None
    if error:
        error_div = Div(
            P(f"✗ {error}", cls="text-red-600 font-bold mb-4 text-center"),
            cls="mb-4",
        )

    return Div(
        # 步骤指示器
        StepIndicator(step),
        # 错误提示（如果有）
        error_div if error else Div(),
        # 表单内容 - 使用 form 包裹以便 HTMX 正确序列化表单数据
        Form(
            Div(content, id="wizard-content-inner"),
            buttons,
            cls="bg-white rounded-lg shadow p-6 max-w-4xl mx-auto",
            id="wizard-form",
        ),
        cls="py-8 px-4",
        id="wizard-form-container",
    )


def InitWizardPage(step: int = 1, form_data: dict | None = None):
    """初始化向导页面（完整页面）"""
    return create_base_page(
        InitWizardForm(step, form_data),
        page_title="系统初始化 - QMT Gateway",
    )
