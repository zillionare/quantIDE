"""账号管理页面"""

from fasthtml.common import *
from monsterui.all import *

from pyqmt.core.enums import BrokerKind
from pyqmt.service.registry import BrokerRegistry
from pyqmt.web.layouts.main import MainLayout

accounts_app, rt = fast_app()


def _get_registry(req) -> BrokerRegistry:
    return req.scope.get("registry")


def LiveAccountCard(account: dict, is_active: bool = False):
    """实盘账号卡片"""
    status_color = "bg-green-100 text-green-700" if account.get("status") else "bg-gray-100 text-gray-500"
    status_text = "已连接" if account.get("status") else "未连接"

    return Div(
        Div(
            Div(
                Div(
                    UkIcon("credit-card", size=24, cls="text-yellow-600"),
                    cls="h-12 w-12 rounded-full bg-yellow-100 flex items-center justify-center",
                ),
                Div(
                    Div(
                        Span("★", cls="text-yellow-500 mr-1") if is_active else "",
                        Span(account.get("name", ""), cls="font-medium text-gray-900"),
                        cls="flex items-center",
                    ),
                    Div(f"ID: {account.get('id', '')[:16]}...", cls="text-sm text-gray-500"),
                    cls="ml-4",
                ),
                cls="flex items-center",
            ),
            Div(
                Div(
                    Div("创建时间", cls="text-xs text-gray-500"),
                    Div(account.get("created_at", "未知"), cls="text-sm text-gray-900"),
                    cls="text-right mr-4",
                ),
                Div(
                    status_text,
                    cls=f"px-3 py-1 rounded-full text-sm {status_color}",
                ),
                Button(
                    "删除",
                    cls="uk-button uk-button-default uk-button-small text-red-600 border-red-600 hover:bg-red-50 ml-2",
                    hx_delete=f"/system/accounts/live/{account.get('id')}",
                    hx_confirm="确定要删除此账号吗？",
                    hx_target="#live-accounts-list",
                ),
                Button(
                    "刷新",
                    cls="uk-button uk-button-default uk-button-small text-blue-600 border-blue-600 hover:bg-blue-50 ml-2",
                    hx_post=f"/system/accounts/live/{account.get('id')}/refresh",
                    hx_target="#live-accounts-list",
                ),
                cls="flex items-center",
            ),
            cls="flex items-center justify-between p-4 border border-gray-200 rounded-lg",
        ),
        cls="mb-3",
    )


def SimAccountCard(account: dict, is_active: bool = False):
    """仿真账号卡片"""
    total = account.get("total", 0)
    principal = account.get("principal", 0)
    pnl = total - principal
    pnl_color = "text-green-600" if pnl >= 0 else "text-red-600"

    return Div(
        Div(
            Div(
                Div(
                    UkIcon("bar-chart-2", size=24, cls="text-blue-600"),
                    cls="h-12 w-12 rounded-full bg-blue-100 flex items-center justify-center",
                ),
                Div(
                    Div(
                        Span("★", cls="text-yellow-500 mr-1") if is_active else "",
                        Span(account.get("name", ""), cls="font-medium text-gray-900"),
                        cls="flex items-center",
                    ),
                    Div(f"创建时间: {account.get('created_at', '未知')}", cls="text-sm text-gray-500"),
                    cls="ml-4",
                ),
                cls="flex items-center",
            ),
            Div(
                Div(
                    Div("初始资金", cls="text-xs text-gray-500"),
                    Div(f"{principal/10000:.2f}万", cls="text-sm font-medium text-gray-900"),
                    cls="text-right mr-6",
                ),
                Div(
                    Div("当前资金", cls="text-xs text-gray-500"),
                    Div(f"{total/10000:.2f}万", cls=f"text-sm font-medium {pnl_color}"),
                    cls="text-right mr-6",
                ),
                Button(
                    "删除",
                    cls="uk-button uk-button-default uk-button-small text-red-600 border-red-600 hover:bg-red-50 ml-2",
                    hx_delete=f"/system/accounts/sim/{account.get('id')}",
                    hx_confirm="确定要删除此仿真账号吗？",
                    hx_target="#sim-accounts-list",
                ),
                Button(
                    "重置",
                    cls="uk-button uk-button-default uk-button-small text-orange-600 border-orange-600 hover:bg-orange-50 ml-2",
                    hx_post=f"/system/accounts/sim/{account.get('id')}/reset",
                    hx_confirm="确定要重置此账号吗？这将清空所有持仓和订单。",
                    hx_target="#sim-accounts-list",
                ),
                cls="flex items-center",
            ),
            cls="flex items-center justify-between p-4 border border-gray-200 rounded-lg",
        ),
        cls="mb-3",
    )


def AccountsPage(live_accounts: list[dict], sim_accounts: list[dict], active_kind: str = "", active_id: str = ""):
    """账号管理页面主内容"""
    return Div(
        # 面包屑导航
        Div(
            Nav(
                A("首页", href="/", cls="text-sm text-gray-600 hover:text-blue-600"),
                Span(" / ", cls="text-gray-400"),
                Span("账号管理", cls="text-sm text-gray-900 font-medium"),
                cls="flex items-center space-x-2 mb-4",
            ),
        ),
        # 页面标题
        Div(
            H1("账号管理", cls="text-2xl font-bold text-gray-900"),
            P("管理您的实盘账号和仿真账号", cls="text-sm text-gray-600 mt-1"),
            cls="mb-6",
        ),
        # 实盘账号区域
        Div(
            Div(
                H2("实盘账号", cls="text-lg font-semibold text-gray-900"),
                cls="px-6 py-4 border-b border-gray-200",
            ),
            Div(
                Div(
                    *[LiveAccountCard(a, is_active=(active_kind == a.get("kind") and active_id == a.get("id"))) for a in live_accounts],
                    id="live-accounts-list",
                ) if live_accounts else P("暂无实盘账号", cls="text-gray-500 italic py-4"),
                # 配置新账号按钮
                Div(
                    A(
                        UkIcon("plus", size=16),
                        " 配置新实盘账号",
                        href="#",
                        cls="flex items-center space-x-2 px-4 py-2 text-sm text-blue-600 border border-dashed border-blue-600 rounded-lg hover:bg-blue-50",
                    ),
                    cls="mt-4",
                ),
                cls="p-6",
            ),
            cls="bg-white rounded-lg shadow mb-6",
        ),
        # 仿真账号区域
        Div(
            Div(
                H2("仿真账号", cls="text-lg font-semibold text-gray-900"),
                Button(
                    "清空所有仿真账号",
                    cls="uk-button uk-button-default uk-button-small text-red-600 border-red-600 hover:bg-red-50",
                    hx_delete="/system/accounts/sim/all",
                    hx_confirm="确定要清空所有仿真账号吗？此操作不可恢复！",
                    hx_target="#sim-accounts-list",
                ),
                cls="px-6 py-4 border-b border-gray-200 flex items-center justify-between",
            ),
            Div(
                Div(
                    *[SimAccountCard(a, is_active=(active_kind == a.get("kind") and active_id == a.get("id"))) for a in sim_accounts],
                    id="sim-accounts-list",
                ) if sim_accounts else P("暂无仿真账号", cls="text-gray-500 italic py-4"),
                # 新建仿真账号按钮
                Div(
                    A(
                        UkIcon("plus", size=16),
                        " 新建仿真账号",
                        href="/trade/simulation",
                        cls="flex items-center space-x-2 px-4 py-2 text-sm text-blue-600 border border-dashed border-blue-600 rounded-lg hover:bg-blue-50",
                    ),
                    cls="mt-4",
                ),
                cls="p-6",
            ),
            cls="bg-white rounded-lg shadow",
        ),
        cls="p-6",
    )


@rt("/")
def accounts_list(req, session):
    """账号管理主页面"""
    layout = MainLayout(title="账号管理", user=session.get("auth"))
    layout.header_active = "交易"
    layout.sidebar_menu = [
        {
            "title": "系统",
            "children": [
                {"title": "账号管理", "url": "/system/accounts", "active": True},
                {"title": "系统设置", "url": "/system"},
            ],
        }
    ]

    reg = _get_registry(req)

    # 获取活动账号
    active_kind = session.get("active_account_kind", "")
    active_id = session.get("active_account_id", "")

    # 获取所有账号
    live_accounts = []
    sim_accounts = []

    if reg:
        # 实盘账号
        for info in reg.list_by_kind(BrokerKind.QMT):
            live_accounts.append({
                "id": info.get("id"),
                "name": info.get("name") or info.get("id"),
                "kind": BrokerKind.QMT.value,
                "status": info.get("status", False),
                "created_at": "未知",  # TODO: 从实际数据获取
            })

        # 仿真账号
        for info in reg.list_by_kind(BrokerKind.SIMULATION):
            broker = reg.get(BrokerKind.SIMULATION, info.get("id"))
            total = 0
            principal = 0
            if broker and hasattr(broker, "asset"):
                total = broker.asset.total if broker.asset else 0
                principal = broker.asset.principal if broker.asset else 0

            sim_accounts.append({
                "id": info.get("id"),
                "name": info.get("name") or info.get("id"),
                "kind": BrokerKind.SIMULATION.value,
                "status": info.get("status", False),
                "created_at": "未知",  # TODO: 从实际数据获取
                "total": total,
                "principal": principal,
            })

    def main_block():
        return AccountsPage(live_accounts, sim_accounts, active_kind, active_id)

    layout.main_block = main_block
    return layout.render()


@rt("/live/{account_id}", methods=["DELETE"])
def delete_live_account(req, account_id: str):
    """删除实盘账号"""
    # TODO: 实现删除逻辑
    return Div("实盘账号删除功能待实现", cls="text-yellow-600 p-4")


@rt("/live/{account_id}/refresh", methods=["POST"])
def refresh_live_account(req, account_id: str):
    """刷新实盘账号状态"""
    # TODO: 实现刷新逻辑
    return Div("实盘账号刷新功能待实现", cls="text-blue-600 p-4")


@rt("/sim/{account_id}", methods=["DELETE"])
def delete_sim_account(req, account_id: str):
    """删除仿真账号"""
    reg = _get_registry(req)
    if reg:
        reg.unregister(BrokerKind.SIMULATION, account_id)
    # 返回空，触发页面刷新
    return ""


@rt("/sim/{account_id}/reset", methods=["POST"])
def reset_sim_account(req, account_id: str):
    """重置仿真账号"""
    reg = _get_registry(req)
    if reg:
        broker = reg.get(BrokerKind.SIMULATION, account_id)
        if broker and hasattr(broker, "reset"):
            broker.reset()
    # 返回空，触发页面刷新
    return ""


@rt("/sim/all", methods=["DELETE"])
def delete_all_sim_accounts(req):
    """清空所有仿真账号"""
    reg = _get_registry(req)
    if reg:
        for info in reg.list_by_kind(BrokerKind.SIMULATION):
            reg.unregister(BrokerKind.SIMULATION, info.get("id"))
    # 返回空，触发页面刷新
    return ""
