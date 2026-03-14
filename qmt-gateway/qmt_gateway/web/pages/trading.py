"""实盘交易界面

根据参考图重构：
1. 顶部：本金、总资产、盈亏、持仓市值、仓位、可用资金
2. 中部：左侧下单表单，右侧 Speed Dial
3. 底部：持仓/委托列表（可切换）
"""

from fasthtml.common import *
from fasthtml.common import Option as HtmlOption
from fasthtml.common import Select as HtmlSelect
from monsterui.all import *

from qmt_gateway.web.layouts.main import create_main_page
from qmt_gateway.web.theme import PRIMARY_COLOR


def AccountInfo(asset: dict | None = None):
    """顶部账号资金信息"""
    if asset is None:
        asset = {
            "principal": 0,
            "total": 0,
            "profit": 0,
            "profit_ratio": 0,
            "market_value": 0,
            "cash": 0,
        }

    principal = asset.get("principal", 0)
    total = asset.get("total", 0)
    profit = asset.get("profit", 0)
    profit_ratio = asset.get("profit_ratio", 0)
    market_value = asset.get("market_value", 0)
    cash = asset.get("cash", 0)

    # 计算占比
    market_value_pct = (market_value / total * 100) if total > 0 else 0
    cash_pct = (cash / total * 100) if total > 0 else 0

    # 盈亏颜色
    profit_color = "#22c55e" if profit >= 0 else "#ef4444"

    return Div(
        Div(
            # 本金
            Div(
                Span("本金 ", cls="text-gray-500"),
                Span(f"{principal/10000:.2f}万", cls="font-medium"),
                cls="flex items-center gap-1",
            ),
            # 总资产
            Div(
                Span("总资产 ", cls="text-gray-500"),
                Span(f"{total/10000:.1f}万", cls="font-medium"),
                cls="flex items-center gap-1",
            ),
            # 盈亏
            Div(
                Span("盈亏 ", cls="text-gray-500"),
                Span(f"{profit/10000:.1f}万/{profit_ratio:.2f}%", style=f"color: {profit_color};"),
                cls="flex items-center gap-1",
            ),
            # 持仓市值（带占比）
            Div(
                Span("持仓市值 ", cls="text-gray-500"),
                Span(f"{market_value/10000:.1f}万 ({market_value_pct:.1f}%)", cls="font-medium"),
                cls="flex items-center gap-1",
            ),
            # 可用资金（带占比）
            Div(
                Span("可用资金 ", cls="text-gray-500"),
                Span(f"{cash/10000:.1f}万 ({cash_pct:.1f}%)", cls="font-medium"),
                cls="flex items-center gap-1",
            ),
            # 刷新按钮
            Button(
                "⟳",
                cls="btn btn-ghost btn-sm",
                hx_get="/api/trade/asset",
                hx_target="#account-info",
            ),
            cls="flex justify-between items-center px-6 py-3 bg-white border-b",
        ),
        id="account-info",
    )


def OrderForm():
    """委托下单表单（左侧）"""
    return Div(
        # 股票搜索（带自动补全）
        Div(
            Div(
                Input(
                    type="text",
                    id="stock-search",
                    name="stock_search",
                    placeholder="请输入股票名、拼音或者代码",
                    cls="input input-bordered w-full pr-10",
                    autocomplete="off",
                    hx_get="/api/stocks/search",
                    hx_trigger="keyup changed delay:200ms",
                    hx_target="#stock-suggestions",
                    hx_swap="innerHTML",
                    hx_indicator="#stock-loading",
                ),
                Button(
                    "🔍",
                    cls="absolute right-2 top-1/2 -translate-y-1/2 btn btn-ghost btn-sm",
                ),
                # 加载指示器
                Div(
                    id="stock-loading",
                    cls="htmx-indicator absolute right-10 top-1/2 -translate-y-1/2",
                ),
                cls="relative",
            ),
            # 自动补全下拉列表
            Div(
                id="stock-suggestions",
                cls="absolute z-50 w-full bg-white rounded shadow-lg mt-1 max-h-60 overflow-y-auto",
            ),
            # 隐藏字段存储选中的股票代码
            Input(type="hidden", id="selected-symbol", name="symbol"),
            Input(type="hidden", id="selected-stock-name", name="stock_name"),
            cls="mb-4 relative",
        ),
        # 显示选中的股票
        Div(
            id="selected-stock-display",
            cls="mb-4 p-2 bg-blue-50 rounded hidden",
        ),
        # 订单类型选择（默认限价单）- 宽度与其他行一致
        Div(
            # 使用原生 select 替代 MonsterUI Select
            Div(
                HtmlSelect(
                    HtmlOption("限价单", value="limit", selected=True),
                    HtmlOption("市价单", value="market"),
                    HtmlOption("本方最优", value="best_own"),
                    HtmlOption("对方最优", value="best_peer"),
                    cls="uk-select select select-bordered w-full min-w-[170px]",
                    onchange="onOrderTypeChange(this.value)",
                    id="order-type",
                    name="order_type",
                ),
                cls="flex-[1.2]",
            ),
            Input(
                type="number",
                step="0.01",
                id="order-price",
                placeholder="委托价格",
                cls="uk-input input input-bordered flex-1 ml-2 text-right min-w-[130px]",
            ),
            cls="flex mb-4 items-center w-full",
        ),
        # 下单方式切换 - 调小尺寸，默认选中按金额
        Div(
            Label(
                Input(type="radio", name="order-mode", value="amount", checked=True, cls="radio radio-sm radio-primary mr-1"),
                Span("按金额下单", cls="text-sm"),
                cls="flex items-center cursor-pointer mr-4",
            ),
            Label(
                Input(type="radio", name="order-mode", value="quantity", cls="radio radio-sm radio-primary mr-1"),
                Span("按数量下单", cls="text-sm"),
                cls="flex items-center cursor-pointer",
            ),
            cls="flex mb-4",
        ),
        # 买入金额（初始空白）
        Div(
            Label("买入金额（万元）", cls="label text-sm"),
            Input(
                type="number",
                id="order-amount",
                placeholder="请输入金额",
                step="0.1",
                cls="input input-bordered w-full",
                oninput="onAmountChange(this.value)",
            ),
            cls="mb-4",
        ),
        # 预估数量（初始空白，可编辑）
        Div(
            Label("预估数量（股）", cls="label text-sm"),
            Input(
                type="number",
                id="est-shares",
                placeholder="请输入数量",
                cls="input input-bordered w-full",
                oninput="onSharesChange(this.value)",
            ),
            cls="mb-4",
        ),
        # 仓位按钮
        Div(
            Button("1/4", cls="btn btn-outline btn-sm flex-1", onclick="setPositionRatio(0.25)"),
            Button("1/3", cls="btn btn-outline btn-sm flex-1 mx-2", onclick="setPositionRatio(0.33)"),
            Button("1/2", cls="btn btn-outline btn-sm flex-1 mr-2", onclick="setPositionRatio(0.5)"),
            Button("全仓", cls="btn btn-outline btn-sm flex-1", onclick="setPositionRatio(1.0)"),
            cls="flex mb-6",
        ),
        # 买入/卖出按钮
        Div(
            Button(
                "买入",
                cls="btn btn-error flex-1 text-white",
                style="background-color: #dc2626;",
            ),
            Button(
                "卖出",
                cls="btn btn-success flex-1 text-white ml-2",
                style="background-color: #16a34a;",
            ),
            cls="flex",
        ),
        cls="p-4 bg-white rounded shadow",
    )


def SpeedDialGrid(last_close: float = 0):
    """Speed Dial 价格网格（右侧）
    
    未选中股票时(last_close=0)，只显示涨跌百分比，不显示价格。
    选中股票后，显示各档位对应的价格。
    按钮为方形设计。
    """
    # 档位配置：(显示数字, 百分比)
    levels = [
        (10, 10), (5, 5), (-1, -1), (-6, -6),
        (9, 9), (4, 4), (-2, -2), (-7, -7),
        (8, 8), (3, 3), (-3, -3), (-8, -8),
        (7, 7), (2, 2), (-4, -4), (-9, -9),
        (6, 6), (1, 1), (-5, -5), (-10, -10),
    ]

    buttons = []
    for display_num, pct in levels:
        # 颜色：正数为红色（涨），负数为绿色（跌）
        if pct > 0:
            color = "bg-red-50 text-red-600 border-red-200 hover:bg-red-100"
        elif pct < 0:
            color = "bg-green-50 text-green-600 border-green-200 hover:bg-green-100"
        else:
            color = "bg-gray-50 text-gray-600 border-gray-200"

        # 如果有昨收价格，计算并显示价格；否则只显示百分比
        if last_close > 0:
            price = last_close * (1 + pct / 100)
            price_text = f"{price:.2f}"
            onclick_action = f"document.getElementById('order-price').value = '{price:.2f}'"
        else:
            price_text = "--"
            onclick_action = ""

        buttons.append(
            Button(
                Div(str(display_num), cls="text-base font-bold leading-tight"),
                Div(price_text, cls="text-xs mt-0.5"),
                cls=f"btn btn-sm aspect-square h-16 w-16 p-0 flex flex-col items-center justify-center border {color}",
                onclick=onclick_action if onclick_action else None,
                disabled=last_close <= 0,
            )
        )

    return Div(
        *buttons,
        cls="grid grid-cols-4 gap-2",
    )


def PositionTabs(active_tab: str = "positions"):
    """持仓/委托切换标签"""
    positions_cls = "btn btn-ghost btn-sm rounded-none"
    orders_cls = "btn btn-ghost btn-sm rounded-none"

    if active_tab == "positions":
        positions_cls += " text-blue-600 border-b-2 border-blue-600"
        orders_cls += " text-gray-500 hover:text-gray-700"
    else:
        positions_cls += " text-gray-500 hover:text-gray-700"
        orders_cls += " text-blue-600 border-b-2 border-blue-600"

    return Div(
        Div(
            Button(
                "我的持仓",
                cls=positions_cls,
                hx_get="/api/trade/positions?view=table",
                hx_target="#positions-orders-container",
            ),
            Button(
                "今日委托",
                cls=orders_cls,
                hx_get="/api/trade/orders?view=table",
                hx_target="#positions-orders-container",
            ),
            cls="flex border-b",
        ),
        cls="mb-4",
    )


def PositionTable(positions: list[dict] | None = None):
    """持仓列表"""
    if positions is None:
        positions = []

    headers = ["代码", "名称", "持有数", "可卖数", "现价", "成本", "盈亏比", "浮盈", "买入均价", "市值", "持有成本", "卖出盈亏", "仓位"]

    rows = []
    if positions:
        for pos in positions:
            profit_ratio = pos.get("profit_ratio", 0)
            profit_color = "text-red-600" if profit_ratio >= 0 else "text-green-600"

            rows.append(
                Tr(
                    Td(pos.get("symbol", "")),
                    Td(pos.get("name", "")),
                    Td(str(pos.get("shares", 0))),
                    Td(str(pos.get("avail", 0))),
                    Td(f"{pos.get('price', 0):.2f}"),
                    Td(f"{pos.get('cost', 0):.2f}"),
                    Td(f"{profit_ratio:.2f}%", cls=profit_color),
                    Td(f"{pos.get('float_profit', 0):.0f}", cls=profit_color),
                    Td(f"{pos.get('buy_avg', 0):.2f}"),
                    Td(f"{pos.get('market_value', 0):.0f}"),
                    Td(f"{pos.get('hold_cost', 0):.0f}"),
                    Td(f"{pos.get('sell_profit', 0):.0f}"),
                    Td(f"{pos.get('position_ratio', 0):.1f}%"),
                    cls="hover:bg-gray-50",
                )
            )
    else:
        rows.append(
            Tr(
                Td("暂无持仓", colspan=len(headers), cls="py-6 text-center text-gray-500"),
                cls="border-t border-gray-200",
            )
        )

    return Div(
        PositionTabs(active_tab="positions"),
        Div(
            Table(
                Thead(
                    Tr(*[Th(h, cls="text-left text-xs font-medium text-gray-500 bg-gray-50") for h in headers])
                ),
                Tbody(*rows),
                cls="w-full text-sm",
            ),
            cls="overflow-x-auto",
        ),
        id="positions-orders-container",
    )


def OrdersTable(orders: list[dict] | None = None):
    """今日委托列表"""
    if orders is None:
        orders = []

    headers = ["时间", "代码", "名称", "方向", "委托价", "委托量", "成交量", "状态", "操作"]

    # 状态映射
    status_map = {
        "pending": ("待成交", "bg-yellow-100 text-yellow-800"),
        "partial": ("部分成交", "bg-blue-100 text-blue-800"),
        "filled": ("已成交", "bg-green-100 text-green-800"),
        "cancelled": ("已撤单", "bg-gray-100 text-gray-600"),
        "rejected": ("已拒绝", "bg-red-100 text-red-800"),
    }

    rows = []
    if orders:
        for order in orders:
            side = order.get("side", "buy")
            side_text = "买入" if side == "buy" else "卖出"
            side_color = "text-red-600" if side == "buy" else "text-green-600"

            status = order.get("status", "pending")
            status_text, status_cls = status_map.get(status, ("未知", "bg-gray-100 text-gray-600"))

            rows.append(
                Tr(
                    Td(order.get("time", "")),
                    Td(order.get("symbol", "")),
                    Td(order.get("name", "")),
                    Td(side_text, cls=side_color),
                    Td(f"{order.get('price', 0):.2f}"),
                    Td(str(order.get("shares", 0))),
                    Td(str(order.get("filled", 0))),
                    Td(Span(status_text, cls=f"inline-flex items-center px-2 py-1 rounded text-xs font-medium {status_cls}")),
                    Td(
                        Button(
                            "撤单",
                            cls="text-blue-600 hover:underline",
                            hx_post=f"/api/trade/cancel?order_id={order.get('qtoid', '')}",
                            hx_confirm="确定要撤单吗？",
                        ) if status in ["pending", "partial"] else "",
                    ),
                    cls="hover:bg-gray-50",
                )
            )
    else:
        rows.append(
            Tr(
                Td("暂无委托", colspan=len(headers), cls="py-6 text-center text-gray-500"),
                cls="border-t border-gray-200",
            )
        )

    return Div(
        PositionTabs(active_tab="orders"),
        Div(
            Table(
                Thead(
                    Tr(*[Th(h, cls="text-left text-xs font-medium text-gray-500 bg-gray-50") for h in headers])
                ),
                Tbody(*rows),
                cls="w-full text-sm",
            ),
            cls="overflow-x-auto",
        ),
        id="positions-orders-container",
    )


def TradingPage(
    asset: dict | None = None,
    positions: list[dict] | None = None,
    orders: list[dict] | None = None,
    trades: list[dict] | None = None,
    user: dict | None = None,
):
    """实盘交易页面"""
    
    # JavaScript 函数
    stock_selection_script = Script("""
        // 选择股票
        window.selectStock = function(symbol, name, lastClose) {
            console.log('[DEBUG] selectStock called:', symbol, name, lastClose);
            
            try {
                // 填充隐藏字段
                document.getElementById('selected-symbol').value = symbol;
                document.getElementById('selected-stock-name').value = name;
                
                // 在搜索框中显示选中的股票
                var searchInput = document.getElementById('stock-search');
                if (searchInput) {
                    searchInput.value = name + ' (' + symbol + ')';
                }
                
                // 隐藏下拉列表
                var suggestions = document.getElementById('stock-suggestions');
                if (suggestions) suggestions.innerHTML = '';
                
                // 更新价格输入框 - 填充现价
                var priceInput = document.getElementById('order-price');
                if (priceInput && lastClose > 0) {
                    priceInput.value = lastClose.toFixed(2);
                }
                
                // 更新 Speed Dial 价格
                if (lastClose > 0) {
                    updateSpeedDial(lastClose);
                }
                
                console.log('[DEBUG] selectStock completed');
            } catch (e) {
                console.error('[DEBUG] selectStock error:', e);
            }
        };
        
        // 更新 Speed Dial 显示价格
        function updateSpeedDial(lastClose) {
            var levels = [
                [10, 10], [5, 5], [-1, -1], [-6, -6],
                [9, 9], [4, 4], [-2, -2], [-7, -7],
                [8, 8], [3, 3], [-3, -3], [-8, -8],
                [7, 7], [2, 2], [-4, -4], [-9, -9],
                [6, 6], [1, 1], [-5, -5], [-10, -10],
            ];
            
            var buttons = document.querySelectorAll('#speed-dial-container button');
            buttons.forEach(function(btn, index) {
                if (index < levels.length) {
                    var pct = levels[index][1];
                    var price = lastClose * (1 + pct / 100);
                    var priceDiv = btn.querySelector('div:last-child');
                    if (priceDiv) {
                        priceDiv.textContent = price.toFixed(2);
                    }
                    btn.disabled = false;
                    btn.onclick = function() {
                        document.getElementById('order-price').value = price.toFixed(2);
                    };
                }
            });
        }
        
        // 获取当前股价
        function getCurrentPrice() {
            var priceInput = document.getElementById('order-price');
            if (priceInput && priceInput.value) {
                return parseFloat(priceInput.value);
            }
            return 0;
        }
        
        // 金额变化时计算数量
        window.onAmountChange = function(amount) {
            var price = getCurrentPrice();
            if (price > 0 && amount) {
                var amountWan = parseFloat(amount);
                var amountYuan = amountWan * 10000;
                var shares = Math.floor(amountYuan / price / 100) * 100; // 向下取整到100股
                document.getElementById('est-shares').value = shares;
            }
        };
        
        // 数量变化时计算金额
        window.onSharesChange = function(shares) {
            var price = getCurrentPrice();
            if (price > 0 && shares) {
                var sharesNum = parseFloat(shares);
                var amountYuan = sharesNum * price;
                var amountWan = amountYuan / 10000;
                document.getElementById('order-amount').value = amountWan.toFixed(2);
            }
        };
        
        // 订单类型切换
        window.onOrderTypeChange = function(orderType) {
            var priceInput = document.getElementById('order-price');
            if (orderType === 'market') {
                priceInput.disabled = true;
                priceInput.placeholder = '市价';
                priceInput.value = '';
            } else {
                priceInput.disabled = false;
                priceInput.placeholder = '委托价格';
            }
        };
        
        // 设置仓位比例
        window.setPositionRatio = function(ratio) {
            // TODO: 根据可用资金计算金额
            console.log('Set position ratio:', ratio);
        };
        
        // 点击页面其他地方时隐藏下拉列表
        document.addEventListener('click', function(e) {
            var searchContainer = document.getElementById('stock-search');
            var suggestions = document.getElementById('stock-suggestions');
            if (searchContainer && suggestions && 
                !searchContainer.contains(e.target) && !suggestions.contains(e.target)) {
                suggestions.innerHTML = '';
            }
        });
    """)
    
    return create_main_page(
        # JavaScript
        stock_selection_script,
        # 第1行：顶部账号资金
        AccountInfo(asset),
        # 第2行：下单表单 + Speed Dial
        Div(
            Div(OrderForm(), cls="w-1/3 pr-4"),
            Div(
                SpeedDialGrid(),
                id="speed-dial-container",
                cls="w-2/3 pl-4",
            ),
            cls="flex px-4 py-4",
        ),
        # 第3行：持仓列表
        Div(
            PositionTable(positions),
            cls="px-4 pb-4",
        ),
        page_title="实盘交易 - QMT Gateway",
        user=user,
    )
