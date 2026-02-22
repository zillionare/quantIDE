"""交易主页面路由

处理用户点击导航栏"交易"时的逻辑：
1. 检查是否有配置任何账号
2. 检查是否有活动账号
3. 根据情况跳转到相应页面
"""

from fasthtml.common import *
from monsterui.all import *
from starlette.responses import RedirectResponse

from pyqmt.core.enums import BrokerKind
from pyqmt.service.registry import BrokerRegistry
from pyqmt.web.layouts.main import MainLayout

trade_index_app, rt = fast_app()


def _get_registry(req) -> BrokerRegistry:
    return req.scope.get("registry")


def _get_all_accounts(reg: BrokerRegistry) -> list[dict]:
    """获取所有账号列表（包括实盘和仿真）"""
    accounts = []
    for kind in [BrokerKind.QMT, BrokerKind.SIMULATION]:
        for info in reg.list_by_kind(kind):
            accounts.append({
                "id": info.get("id"),
                "name": info.get("name") or info.get("id"),
                "kind": kind.value,
                "label": "实盘" if kind == BrokerKind.QMT else "仿真",
                "status": info.get("status", False),
                "is_live": kind == BrokerKind.QMT,
            })
    return accounts


def _get_active_account(reg: BrokerRegistry, session: dict) -> dict | None:
    """获取当前活动账号

    优先从 session 中获取，如果没有则从 registry 获取默认账号
    """
    # 首先尝试从 session 获取
    kind_str = session.get("active_account_kind")
    account_id = session.get("active_account_id")

    # 如果 session 中没有，尝试从 registry 获取默认账号
    if not kind_str or not account_id:
        default = reg.get_default() if reg else None
        if not default:
            return None
        kind_str, account_id = default

    kind = BrokerKind(kind_str) if isinstance(kind_str, str) else kind_str
    broker = reg.get(kind, account_id) if reg else None

    if broker:
        name = ""
        status = True
        if hasattr(broker, "portfolio_name"):
            name = broker.portfolio_name
        if hasattr(broker, "status"):
            status = broker.status
        return {
            "id": account_id,
            "name": name or account_id,
            "kind": kind_str if isinstance(kind_str, str) else kind_str.value,
            "label": "实盘" if (kind == BrokerKind.QMT or kind_str == BrokerKind.QMT.value) else "仿真",
            "status": status,
            "is_live": kind == BrokerKind.QMT or kind_str == BrokerKind.QMT.value,
        }
    return None


def NoAccountPage():
    """无账号提示页面"""
    return Div(
        Div(
            Div(
                UkIcon("alert-circle", size=48, cls="text-yellow-500 mb-4"),
                H2("暂无交易账号", cls="text-2xl font-bold text-gray-900 mb-4"),
                P("您还没有配置任何交易账号。请先创建或配置账号。", cls="text-gray-600 mb-6"),
                Div(
                    A(
                        UkIcon("plus", size=16),
                        " 创建仿真账号",
                        href="/trade/simulation",
                        cls="uk-button uk-button-primary mr-4",
                        style="background-color: #d32f2f;",
                    ),
                    A(
                        UkIcon("settings", size=16),
                        " 账号管理",
                        href="/system/accounts",
                        cls="uk-button uk-button-default",
                    ),
                    cls="flex justify-center gap-4",
                ),
                cls="text-center py-16",
            ),
            cls="max-w-2xl mx-auto bg-white rounded-xl shadow-sm border border-gray-100 p-8",
        ),
        cls="p-6",
    )


def SelectAccountPage(accounts: list[dict]):
    """选择活动账号页面"""
    live_accounts = [a for a in accounts if a["is_live"]]
    sim_accounts = [a for a in accounts if not a["is_live"]]

    def AccountCard(account: dict):
        return Div(
            Div(
                Div(
                    Span(
                        "实盘" if account["is_live"] else "仿真",
                        cls=f"px-2 py-1 text-xs font-medium rounded-full {'bg-red-100 text-red-700' if account['is_live'] else 'bg-blue-100 text-blue-700'}"
                    ),
                    Span(
                        "在线" if account["status"] else "离线",
                        cls=f"ml-2 text-xs {'text-green-600' if account['status'] else 'text-gray-400'}"
                    ),
                    cls="mb-2",
                ),
                H4(account["name"], cls="text-lg font-semibold text-gray-900"),
                P(f"ID: {account['id'][:16]}...", cls="text-xs text-gray-500"),
                cls="mb-4",
            ),
            Form(
                Input(type="hidden", name="kind", value=account["kind"]),
                Input(type="hidden", name="id", value=account["id"]),
                Button(
                    "设为活动账号",
                    type="submit",
                    cls="uk-button uk-button-primary w-full",
                    style="background-color: #d32f2f;",
                ),
                hx_post="/trade/set-active",
                hx_target="body",
                hx_swap="outerHTML",
            ),
            cls="bg-white rounded-xl shadow-sm border border-gray-100 p-6 hover:shadow-md transition-shadow",
        )

    return Div(
        Div(
            H2("选择活动账号", cls="text-2xl font-bold text-gray-900 mb-2"),
            P("请选择一个账号作为当前活动账号，所有交易操作将与该账号关联。", cls="text-gray-600 mb-6"),
            cls="mb-6",
        ),
        Div(
            Div(
                H3("实盘账号", cls="text-lg font-semibold text-gray-900 mb-4"),
                Div(
                    *[AccountCard(a) for a in live_accounts],
                    cls="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4",
                ) if live_accounts else P("暂无实盘账号", cls="text-gray-500 italic"),
                cls="mb-8",
            ),
            Div(
                H3("仿真账号", cls="text-lg font-semibold text-gray-900 mb-4"),
                Div(
                    *[AccountCard(a) for a in sim_accounts],
                    cls="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4",
                ) if sim_accounts else P("暂无仿真账号", cls="text-gray-500 italic"),
                cls="mb-8",
            ),
            Div(
                A(
                    UkIcon("settings", size=16),
                    " 管理账号",
                    href="/system/accounts",
                    cls="uk-button uk-button-default",
                ),
                cls="flex justify-center",
            ),
            cls="max-w-6xl mx-auto",
        ),
        cls="p-6",
    )


@rt("/")
@rt("")
def trade_index(req, session):
    """交易主页面

    逻辑：
    1. 检查是否有任何账号配置
    2. 如果没有，显示无账号提示页面
    3. 检查是否有活动账号
    4. 如果没有活动账号，显示选择账号页面
    5. 如果有活动账号，根据账号类型跳转到相应页面
    """
    reg = _get_registry(req)

    # 获取所有账号
    accounts = _get_all_accounts(reg) if reg else []

    # 如果没有配置任何账号
    if not accounts:
        layout = MainLayout(title="交易 - 无账号", user=session.get("auth"))
        layout.header_active = "交易"
        layout.main_block = lambda: NoAccountPage()
        return layout.render()

    # 获取活动账号
    active_account = _get_active_account(reg, session)

    # 如果没有活动账号，显示选择页面
    if not active_account:
        layout = MainLayout(title="交易 - 选择账号", user=session.get("auth"))
        layout.header_active = "交易"
        layout.main_block = lambda: SelectAccountPage(accounts)
        return layout.render()

    # 有活动账号，根据类型跳转到相应页面
    if active_account["is_live"]:
        return RedirectResponse(url=f"/trade/live/{active_account['id']}")
    else:
        return RedirectResponse(url=f"/trade/simulation/{active_account['id']}")


@rt("/set-active", methods=["POST"])
async def set_active_account(req, session):
    """设置活动账号

    将选中的账号保存到 session 中作为活动账号
    """
    form = await req.form()
    kind = form.get("kind")
    account_id = form.get("id")

    if not kind or not account_id:
        return Div("参数错误", cls="text-red-500 p-4")

    # 将活动账号保存到 session
    session["active_account_kind"] = kind
    session["active_account_id"] = account_id

    # 设置完成后重定向到交易首页
    return RedirectResponse(url="/trade", status_code=303)
