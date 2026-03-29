"""账号管理页面"""

import uuid
from datetime import datetime

from fasthtml.common import *
from loguru import logger
from monsterui.all import *
from starlette.responses import HTMLResponse, RedirectResponse

from quantide.core.enums import BrokerKind
from quantide.data.sqlite import db
from quantide.service.registry import BrokerRegistry
from quantide.service.sim_broker import PaperBroker
from quantide.web.layouts.main import MainLayout

from quantide.web.theme import AppTheme

accounts_app, rt = fast_app(hdrs=AppTheme.headers())


def _get_registry(req) -> BrokerRegistry:
    return req.scope.get("registry")


def _get_market_data(req):
    runtime = getattr(req.app.state, "runtime", None)
    if runtime is None:
        return None
    return getattr(runtime, "market_data", None)


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
                    type="button",
                    cls="text-sm text-red-600 hover:text-red-800 px-3 py-1 border border-red-600 rounded hover:bg-red-50",
                    onclick=f"document.getElementById('delete-dialog-{account.get('id')}').classList.remove('hidden')",
                ),
                ConfirmDialogModal(
                    dialog_id=f"delete-dialog-{account.get('id')}",
                    title="删除账户",
                    message=f"确定要删除账户 \"{account.get('name')}\" 吗？此操作不可恢复。",
                    confirm_url=f"/system/accounts/sim/{account.get('id')}/delete",
                ),
                Button(
                    "重置",
                    type="button",
                    cls="text-sm text-orange-600 hover:text-orange-800 px-3 py-1 border border-orange-600 rounded hover:bg-orange-50 ml-2",
                    onclick=f"document.getElementById('reset-dialog-{account.get('id')}').classList.remove('hidden')",
                ),
                ConfirmDialogModal(
                    dialog_id=f"reset-dialog-{account.get('id')}",
                    title="重置账户",
                    message=f"确定要重置账户 \"{account.get('name')}\" 吗？这将清空所有持仓和订单。",
                    confirm_url=f"/system/accounts/sim/{account.get('id')}/reset",
                ),
                cls="flex items-center",
            ),
            cls="flex items-center justify-between p-4 border border-gray-200 rounded-lg",
        ),
        cls="mb-3",
    )


def ConfirmDialogModal(dialog_id: str, title: str, message: str, confirm_url: str, confirm_method: str = "POST"):
    """确认对话框组件"""
    return Div(
        Div(
            H3(title, cls="text-lg font-semibold text-gray-900 mb-2"),
            P(message, cls="text-sm text-gray-600 mb-4"),
            Div(
                Form(
                    Button(
                        "确定",
                        type="submit",
                        cls="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700",
                    ),
                    action=confirm_url,
                    method=confirm_method,
                    cls="inline",
                ),
                Button(
                    "取消",
                    type="button",
                    onclick=f"document.getElementById('{dialog_id}').classList.add('hidden')",
                    cls="ml-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50",
                ),
                cls="flex justify-end",
            ),
            cls="bg-white p-6 rounded-lg shadow-lg max-w-md w-full",
        ),
        id=dialog_id,
        cls="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 hidden",
    )


def AccountsPage(live_accounts: list[dict], sim_accounts: list[dict], active_kind: str = "", active_id: str = "", show_create_modal: bool = False):
    """账号管理页面主内容"""
    # 弹窗显示脚本
    modal_script = Script("""
        document.addEventListener('DOMContentLoaded', function() {
            var modal = document.getElementById('create-sim-modal');
            if (modal && %s) {
                modal.classList.remove('hidden');
            }
        });
    """ % ("true" if show_create_modal else "false"))

    return Div(
        modal_script,
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
            P("管理您的实盘账户和模拟交易账户", cls="text-sm text-gray-600 mt-1"),
            cls="mb-6",
        ),
        # 实盘账户区域
        Div(
            Div(
                H2("实盘账户", cls="text-lg font-semibold text-gray-900"),
                cls="px-6 py-4 border-b border-gray-200",
            ),
            Div(
                Div(
                    *[LiveAccountCard(a, is_active=(active_kind == a.get("kind") and active_id == a.get("id"))) for a in live_accounts],
                    id="live-accounts-list",
                ) if live_accounts else P("暂无实盘账户", cls="text-gray-500 italic py-4"),
                # 配置新账户按钮
                Div(
                    A(
                        UkIcon("plus", size=16),
                        " 配置新实盘账户",
                        href="#",
                        cls="flex items-center space-x-2 px-4 py-2 text-sm text-blue-600 border border-dashed border-blue-600 rounded-lg hover:bg-blue-50",
                    ),
                    cls="mt-4",
                ),
                cls="p-6",
            ),
            cls="bg-white rounded-lg shadow mb-6",
        ),
        # 模拟交易账户区域
        Div(
            Div(
                H2("模拟交易账户", cls="text-lg font-semibold text-gray-900"),
                Button(
                    "清空所有模拟交易账户",
                    type="button",
                    cls="text-sm text-red-600 hover:text-red-800 px-3 py-1 border border-red-600 rounded hover:bg-red-50",
                    onclick="document.getElementById('delete-all-dialog').classList.remove('hidden')",
                ),
                ConfirmDialogModal(
                    dialog_id="delete-all-dialog",
                    title="清空所有账户",
                    message="确定要清空所有模拟交易账户吗？此操作不可恢复！",
                    confirm_url="/system/accounts/sim/all/delete",
                ),
                cls="px-6 py-4 border-b border-gray-200 flex items-center justify-between",
            ),
            Div(
                Div(
                    *[SimAccountCard(a, is_active=(active_kind == a.get("kind") and active_id == a.get("id"))) for a in sim_accounts],
                    id="sim-accounts-list",
                ) if sim_accounts else P("暂无模拟交易账户", cls="text-gray-500 italic py-4"),
                # 新建模拟交易账户按钮
                Div(
                    Button(
                        UkIcon("plus", size=16),
                        " 新建模拟交易账户",
                        type="button",
                        onclick="document.getElementById('create-sim-modal').classList.remove('hidden')",
                        cls="flex items-center space-x-2 px-4 py-2 text-sm text-blue-600 border border-dashed border-blue-600 rounded-lg hover:bg-blue-50",
                    ),
                    cls="mt-4",
                ),
                # 创建模拟交易账户弹窗
                CreateSimAccountForm(),
                cls="p-6",
            ),
            cls="bg-white rounded-lg shadow",
        ),
        cls="p-6",
    )


def accounts_list(request):
    """账号管理主页面"""
    session = request.scope.get("session", {})
    layout = MainLayout(title="账号管理", user=session.get("auth"))
    layout.header_active = "交易"
    layout.set_sidebar_active("/system/accounts")

    reg = _get_registry(request)

    # 获取活动账号
    active_kind = session.get("active_account_kind", "")
    active_id = session.get("active_account_id", "")

    # 获取所有账号
    live_accounts = []
    sim_accounts = []

    if reg:
        # 实盘账户
        for info in reg.list_by_kind(BrokerKind.QMT):
            account_id = info.get("id")
            if not account_id:
                continue
            broker = reg.get(BrokerKind.QMT, account_id)
            total = 0
            principal = 0
            if broker:
                # QMTBroker 使用 asset 属性
                asset = getattr(broker, "asset", None)
                if asset:
                    total = getattr(asset, "total", 0)
                    principal = getattr(asset, "principal", 0)

            live_accounts.append({
                "id": account_id,
                "name": info.get("name") or account_id,
                "kind": BrokerKind.QMT.value,
                "status": info.get("status", False),
                "created_at": datetime.now().strftime("%Y-%m-%d"),
                "total": total,
                "principal": principal,
            })

        # 模拟交易账户
        for info in reg.list_by_kind(BrokerKind.SIMULATION):
            account_id = info.get("id")
            if not account_id:
                continue
            broker = reg.get(BrokerKind.SIMULATION, account_id)
            total = 0
            principal = 0
            if broker:
                # SimulationBroker 使用 principal 和 total_assets 属性
                principal = getattr(broker, "principal", 0)
                total = getattr(broker, "total_assets", 0)

            sim_accounts.append({
                "id": account_id,
                "name": info.get("name") or account_id,
                "kind": BrokerKind.SIMULATION.value,
                "status": info.get("status", False),
                "created_at": datetime.now().strftime("%Y-%m-%d"),
                "total": total,
                "principal": principal,
            })

    # 检查是否需要显示创建弹窗
    show_create_modal = request.query_params.get("create_sim") == "1"

    def main_block():
        return AccountsPage(live_accounts, sim_accounts, active_kind, active_id, show_create_modal)

    layout.main_block = main_block
    return HTMLResponse(to_xml(layout.render()))


@rt("/")
def accounts_index(request):
    """账号管理主页面 - 根路径"""
    return accounts_list(request)


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


def _auto_select_latest_account(reg, session):
    """自动选择最新的账户作为活动账户"""
    if not reg:
        return

    # 获取所有模拟账户
    sim_accounts = []
    for info in reg.list_by_kind(BrokerKind.SIMULATION):
        account_id = info.get("id")
        if account_id:
            # 从数据库获取创建时间
            pf = db.get_portfolio(account_id)
            if pf:
                sim_accounts.append({
                    "id": account_id,
                    "kind": BrokerKind.SIMULATION.value,
                    "start": pf.start,
                })

    if sim_accounts:
        # 按创建时间排序，选择最新的
        sim_accounts.sort(key=lambda x: x.get("start") or "", reverse=True)
        latest = sim_accounts[0]
        session["active_account_kind"] = latest["kind"]
        session["active_account_id"] = latest["id"]
        return latest

    # 没有模拟账户，尝试实盘账户
    live_accounts = reg.list_by_kind(BrokerKind.QMT)
    if live_accounts:
        live = live_accounts[0]
        session["active_account_kind"] = BrokerKind.QMT.value
        session["active_account_id"] = live.get("id")
        return {"id": live.get("id"), "kind": BrokerKind.QMT.value}

    # 没有任何账户
    session["active_account_kind"] = ""
    session["active_account_id"] = ""
    return None


@rt("/sim/all/delete", methods=["POST"])
def delete_all_sim_accounts(req):
    """清空所有仿真账号"""
    logger.info("DELETE ALL request received")

    reg = _get_registry(req)
    session = req.scope.get("session", {})

    if reg:
        for info in reg.list_by_kind(BrokerKind.SIMULATION):
            account_id = info.get("id")
            if account_id:
                reg.unregister(BrokerKind.SIMULATION, account_id)
                # 从数据库删除
                try:
                    db.delete_portfolio(account_id)
                    logger.info(f"Deleted portfolio: {account_id}")
                except Exception as e:
                    print(f"Failed to delete portfolio {account_id} from db: {e}")

    # 清除活动账户
    session["active_account_kind"] = ""
    session["active_account_id"] = ""

    logger.info("All accounts deleted")

    # 重定向到账户列表页面
    return RedirectResponse(url="/system/accounts", status_code=303)


@rt("/sim/{account_id}/delete", methods=["POST"])
def delete_sim_account(req, account_id: str):
    """删除仿真账号"""
    logger.info(f"DELETE request received for account: {account_id}")

    reg = _get_registry(req)
    session = req.scope.get("session", {})
    active_kind = session.get("active_account_kind", "")
    active_id = session.get("active_account_id", "")

    logger.info(f"Registry: {reg}, Active: {active_kind}:{active_id}")

    if reg:
        reg.unregister(BrokerKind.SIMULATION, account_id)
        logger.info(f"Unregistered account: {account_id}")

    # 从数据库删除 portfolio
    try:
        db.delete_portfolio(account_id)
    except Exception as e:
        print(f"Failed to delete portfolio from db: {e}")

    # 如果删除的是当前活动账户，自动选择最新账户
    if active_kind == BrokerKind.SIMULATION.value and active_id == account_id:
        _auto_select_latest_account(reg, session)

    # 重定向到账户列表页面
    return RedirectResponse(url="/system/accounts", status_code=303)


@rt("/sim/{account_id}/reset", methods=["POST"])
def reset_sim_account(req, account_id: str):
    """重置仿真账号"""
    logger.info(f"RESET request received for account: {account_id}")

    reg = _get_registry(req)
    if reg:
        broker = reg.get(BrokerKind.SIMULATION, account_id)
        if broker and hasattr(broker, "reset"):
            broker.reset()
            logger.info(f"Reset account: {account_id}")

    # 重定向到账户列表页面
    return RedirectResponse(url="/system/accounts", status_code=303)


def CreateSimAccountForm():
    """创建模拟交易账户表单"""
    return Div(
        Div(
            H3("新建模拟交易账户", cls="text-lg font-semibold text-gray-900 mb-4"),
            Form(
                Div(
                    Span("账户名称", cls="block text-sm text-gray-700 mb-1"),
                    Input(type="text", name="name", placeholder="例如：模拟账户A", required=True,
                          cls="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"),
                    cls="mb-4",
                ),
                Div(
                    Span("初始资金（万元）", cls="block text-sm text-gray-700 mb-1"),
                    Input(type="text", name="principal", value="100", required=True,
                          cls="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"),
                    cls="mb-4",
                ),
                Div(
                    Button("创建", type="submit",
                           cls="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"),
                    Button("取消", type="button", onclick="document.getElementById('create-sim-modal').classList.add('hidden')",
                           cls="ml-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50"),
                    cls="flex justify-end",
                ),
                method="POST",
                action="/system/accounts/sim/create",
                hx_post="/system/accounts/sim/create",
                hx_target="#sim-accounts-list",
                hx_swap="outerHTML",
                cls="space-y-4",
            ),
            cls="bg-white p-6 rounded-lg shadow-lg max-w-md w-full",
        ),
        id="create-sim-modal",
        cls="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 hidden",
    )


def _get_sim_accounts_list(reg, active_kind: str = "", active_id: str = ""):
    """获取模拟交易账户列表HTML"""
    sim_accounts = []
    if reg:
        for info in reg.list_by_kind(BrokerKind.SIMULATION):
            account_id = info.get("id")
            if not account_id:
                continue
            broker = reg.get(BrokerKind.SIMULATION, account_id)
            total = 0
            principal = 0
            if broker:
                # SimulationBroker 使用 principal 和 total_assets 属性
                principal = getattr(broker, "principal", 0)
                total = getattr(broker, "total_assets", 0)

            sim_accounts.append({
                "id": account_id,
                "name": info.get("name") or account_id,
                "kind": BrokerKind.SIMULATION.value,
                "status": info.get("status", False),
                "created_at": datetime.now().strftime("%Y-%m-%d"),
                "total": total,
                "principal": principal,
            })

    return Div(
        Div(
            *[SimAccountCard(a, is_active=(active_kind == a.get("kind") and active_id == a.get("id"))) for a in sim_accounts],
            id="sim-accounts-list",
        ) if sim_accounts else P("暂无模拟交易账户", cls="text-gray-500 italic py-4"),
        # 新建模拟交易账户按钮
        Div(
            Button(
                UkIcon("plus", size=16),
                " 新建模拟交易账户",
                type="button",
                onclick="document.getElementById('create-sim-modal').classList.remove('hidden')",
                cls="flex items-center space-x-2 px-4 py-2 text-sm text-blue-600 border border-dashed border-blue-600 rounded-lg hover:bg-blue-50",
            ),
            cls="mt-4",
        ),
        # 创建模拟交易账户弹窗
        CreateSimAccountForm(),
    )


@rt("/sim/create", methods=["POST"])
async def create_sim_account(req):
    """创建模拟交易账户"""
    reg = _get_registry(req)
    if not reg:
        return Div("系统错误：无法访问账户注册表", cls="text-red-500 p-4")

    form = await req.form()
    name = form.get("name", "").strip()
    principal_str = form.get("principal", "100")

    if not name:
        return Div("账户名称不能为空", cls="text-red-500 p-4")

    # 检查账户名是否已存在
    for info in reg.list_by_kind(BrokerKind.SIMULATION):
        if info.get("name") == name:
            # 名称已存在，重定向到账户列表页面
            return RedirectResponse(url="/system/accounts", status_code=303)

    try:
        principal = float(principal_str) * 10000  # 转换为元
    except ValueError:
        principal = 1000000  # 默认100万

    # 生成唯一ID
    account_id = f"sim_{uuid.uuid4().hex[:8]}"

    try:
        market_data = _get_market_data(req)
        # 创建模拟交易账户（使用 create 方法会自动保存到数据库）
        sim_broker = PaperBroker.create(
            portfolio_id=account_id,
            portfolio_name=name,
            principal=principal,
            market_data=market_data,
        )
        runtime = getattr(req.app.state, "runtime", None)
        if runtime is not None:
            runtime.register_legacy_broker(
                broker=sim_broker,
                portfolio_id=account_id,
                kind=BrokerKind.SIMULATION,
                portfolio_name=name,
                status=True,
                is_connected=True,
            )
        else:
            reg.register(BrokerKind.SIMULATION, account_id, sim_broker)

        # 设置为活动账户
        session = req.scope.get("session", {})
        session["active_account_kind"] = BrokerKind.SIMULATION.value
        session["active_account_id"] = account_id

        # 创建成功，重定向到账户列表页面
        return RedirectResponse(url="/system/accounts", status_code=303)
    except Exception as e:
        return Div(f"创建失败：{str(e)}", cls="text-red-500 p-4")
