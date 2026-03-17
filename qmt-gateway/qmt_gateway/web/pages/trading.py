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


def OrderForm(available_cash: float = 0):
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
                    oninput="if(window.onStockInputChange){window.onStockInputChange(this.value);}",
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
                Input(
                    type="radio",
                    id="order-mode-amount",
                    name="order-mode",
                    value="amount",
                    checked=True,
                    cls="mr-2 h-4 w-4 accent-blue-600",
                    onchange="onOrderModeChange()",
                ),
                Span("按金额下单", cls="text-sm"),
                cls="flex items-center cursor-pointer mr-4",
            ),
            Label(
                Input(
                    type="radio",
                    id="order-mode-quantity",
                    name="order-mode",
                    value="quantity",
                    cls="mr-2 h-4 w-4 accent-blue-600",
                    onchange="onOrderModeChange()",
                ),
                Span("按数量下单", cls="text-sm"),
                cls="flex items-center cursor-pointer",
            ),
            cls="flex mb-4",
        ),
        Div(
            Label("下单值", cls="label text-sm", id="order-value-label"),
            Div(
                Input(
                    type="number",
                    id="order-value",
                    placeholder="请输入金额",
                    step="0.1",
                    cls="input input-bordered w-full",
                    oninput="refreshOrderEstimate()",
                ),
                Span("万元", id="order-value-unit", cls="text-gray-500 text-sm ml-2 whitespace-nowrap"),
                cls="flex items-center",
            ),
            Div(
                Span("预估手数：", id="order-estimate-label", cls="text-sm text-gray-500"),
                Span("--", id="order-estimate", cls="text-sm font-medium ml-1"),
                cls="mt-2",
            ),
            Input(type="hidden", id="est-shares"),
            Input(type="hidden", id="available-cash", value=f"{float(available_cash or 0):.2f}"),
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
        if pct > 0:
            pct_style = "color: #dc2626;"
        elif pct < 0:
            pct_style = "color: #16a34a;"
        else:
            pct_style = "color: #4b5563;"

        if last_close > 0:
            price = last_close * (1 + pct / 100)
            price_text = f"{price:.2f}"
            price_cls = "speed-dial-price absolute right-2 bottom-1 text-[11px] font-medium text-gray-400 leading-none"
            onclick_action = (
                f"if(window.applySpeedDialPrice){{window.applySpeedDialPrice({price:.2f});}}"
            )
            clickable_cls = "cursor-pointer"
        else:
            price_text = ""
            price_cls = "speed-dial-price hidden"
            onclick_action = ""
            clickable_cls = "cursor-default"

        buttons.append(
            Button(
                Span(
                    f"{display_num:+d}%",
                    cls="speed-dial-pct text-[24px] font-medium italic leading-none",
                    style=pct_style,
                ),
                Span(
                    price_text,
                    cls=price_cls,
                ),
                cls=(
                    "btn h-32 w-32 min-h-0 rounded-xl p-0 relative bg-white "
                    f"flex items-center justify-center border border-gray-200 shadow-sm {clickable_cls}"
                ),
                onclick=onclick_action if onclick_action else None,
            )
        )

    return Div(
        *buttons,
        cls="grid grid-cols-4 gap-x-2 gap-y-1",
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

    headers = ["代码", "名称", "持有数", "可卖数", "现价", "成本", "盈亏比", "浮盈", "市值", "持有成本", "卖出盈亏", "仓位"]

    rows = []
    if positions:
        for pos in positions:
            profit_ratio = pos.get("profit_ratio", 0)
            profit_color = "text-red-600" if profit_ratio >= 0 else "text-green-600"
            symbol = str(pos.get("symbol", ""))
            name = str(pos.get("name", ""))
            price = float(pos.get("price", 0))
            avail = float(pos.get("avail", 0))
            can_sell = avail > 0
            row_attrs = {}
            row_cls = "hover:bg-gray-50"
            if can_sell and symbol:
                safe_symbol = symbol.replace("\\", "\\\\").replace("'", "\\'")
                safe_name = name.replace("\\", "\\\\").replace("'", "\\'")
                row_attrs["ondblclick"] = (
                    "window.fillSellFromPosition("
                    f"'{safe_symbol}',"
                    f"'{safe_name}',"
                    f"{price},"
                    f"{avail}"
                    ");"
                )
                row_cls += " cursor-pointer"

            rows.append(
                Tr(
                    Td(symbol),
                    Td(name),
                    Td(str(pos.get("shares", 0))),
                    Td(str(avail)),
                    Td(f"{price:.2f}"),
                    Td(f"{pos.get('cost', 0):.4f}"),
                    Td(f"{profit_ratio:.2f}%", cls=profit_color),
                    Td(f"{pos.get('float_profit', 0):.0f}", cls=profit_color),
                    Td(f"{pos.get('market_value', 0):.0f}"),
                    Td(f"{pos.get('hold_cost', 0):.0f}"),
                    Td(f"{pos.get('sell_profit', 0):.0f}"),
                    Td(f"{pos.get('position_ratio', 0):.1f}%"),
                    cls=row_cls,
                    **row_attrs,
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
        Script("""
            window.fillSellFromPosition = function(symbol, name, price, avail) {
                if (!symbol || !avail || Number(avail) <= 0) {
                    return;
                }
                var searchInput = document.getElementById('stock-search');
                var symbolInput = document.getElementById('selected-symbol');
                var nameInput = document.getElementById('selected-stock-name');
                var priceInput = document.getElementById('order-price');
                var modeAmount = document.getElementById('order-mode-amount');
                var modeQuantity = document.getElementById('order-mode-quantity');
                var orderValueInput = document.getElementById('order-value');
                if (searchInput) {
                    searchInput.value = (name || symbol) + ' (' + symbol + ')';
                }
                if (symbolInput) {
                    symbolInput.value = symbol;
                }
                if (nameInput) {
                    nameInput.value = name || symbol;
                }
                if (priceInput && Number(price) > 0) {
                    priceInput.value = Number(price).toFixed(2);
                }
                if (modeQuantity) {
                    modeQuantity.checked = true;
                }
                if (modeAmount) {
                    modeAmount.checked = false;
                }
                if (window.onOrderModeChange) {
                    window.onOrderModeChange();
                }
                if (orderValueInput) {
                    orderValueInput.value = String(Math.floor(Number(avail) / 100));
                }
                if (window.refreshOrderEstimate) {
                    window.refreshOrderEstimate();
                }
            };
        """),
        id="positions-orders-container",
        hx_get="/api/trade/positions?view=table",
        hx_trigger="every 5s",
        hx_target="#positions-orders-container",
        hx_swap="outerHTML",
    )


def OrdersTable(orders: list[dict] | None = None):
    """今日委托列表"""
    if orders is None:
        orders = []

    headers = ["时间", "代码", "名称", "方向", "委托价", "委托量", "成交量", "状态", "操作"]

    # 状态映射
    status_map = {
        "unreported": ("未报", "bg-gray-100 text-gray-600"),
        "pending": ("待成交", "bg-yellow-100 text-yellow-800"),
        "reported": ("已报", "bg-blue-100 text-blue-800"),
        "canceling": ("已报待撤", "bg-orange-100 text-orange-800"),
        "partial_canceling": ("部成待撤", "bg-orange-100 text-orange-800"),
        "partial_cancelled": ("部撤", "bg-gray-100 text-gray-600"),
        "partial": ("部分成交", "bg-blue-100 text-blue-800"),
        "filled": ("已成交", "bg-green-100 text-green-800"),
        "cancelled": ("已撤单", "bg-gray-100 text-gray-600"),
        "rejected": ("废单", "bg-red-100 text-red-800"),
        "unknown": ("未知", "bg-gray-100 text-gray-600"),
    }

    rows = []
    if orders:
        for order in orders:
            side = order.get("side", "buy")
            side_text = "买入" if side == "buy" else "卖出"
            side_color = "text-red-600" if side == "buy" else "text-green-600"

            status = order.get("status", "pending")
            status_text, status_cls = status_map.get(status, ("未知", "bg-gray-100 text-gray-600"))
            can_cancel = bool(order.get("can_cancel", False)) and status != "filled"
            order_id = str(order.get("qtoid", ""))
            row_attrs = {}
            row_cls = "hover:bg-gray-50"
            if can_cancel and order_id:
                row_attrs["ondblclick"] = f"window.cancelOrder('{order_id}', true);"
                row_cls += " cursor-pointer"

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
                            onclick=f"window.cancelOrder('{order_id}', true); return false;",
                        ) if can_cancel and order_id else "",
                    ),
                    cls=row_cls,
                    **row_attrs,
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
        Script("""
            window.cancelOrder = function(orderId, needConfirm) {
                if (!orderId) {
                    return;
                }
                if (needConfirm && !window.confirm('确定要撤单吗？')) {
                    return;
                }
                htmx.ajax('POST', '/api/trade/cancel?view=table&order_id=' + encodeURIComponent(orderId), {
                    target: '#positions-orders-container',
                    swap: 'outerHTML'
                });
            };
        """),
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
        window.applySpeedDialPrice = function(price) {
            if (!(Number(price) > 0)) {
                return;
            }
            var orderType = document.getElementById('order-type');
            if (orderType && orderType.value !== 'limit') {
                orderType.value = 'limit';
            }
            if (window.onOrderTypeChange) {
                window.onOrderTypeChange('limit');
            }
            var priceInput = document.getElementById('order-price');
            if (priceInput) {
                priceInput.value = Number(price).toFixed(2);
            }
            if (window.refreshOrderEstimate) {
                window.refreshOrderEstimate();
            }
        };

        function fetchAndRenderSpeedDial(symbol) {
            var container = document.getElementById('speed-dial-container');
            if (!container || !symbol) {
                return;
            }
            fetch('/api/stock/info?symbol=' + encodeURIComponent(symbol))
                .then(function(resp) { return resp.text(); })
                .then(function(html) { container.innerHTML = html; })
                .catch(function() {});
        }

        function resetSpeedDial() {
            var container = document.getElementById('speed-dial-container');
            if (!container) {
                return;
            }
            fetch('/api/stock/info')
                .then(function(resp) { return resp.text(); })
                .then(function(html) { container.innerHTML = html; })
                .catch(function() {});
        }

        function parseSymbolFromInputValue(raw) {
            if (!raw) {
                return '';
            }
            var text = String(raw).trim().toUpperCase();
            var match = text.match(/([0-9]{6}[.](?:SH|SZ|BJ))/);
            if (match && match[1]) {
                return match[1];
            }
            var six = text.match(/^([0-9]{6})$/);
            if (six && six[1]) {
                return six[1];
            }
            return '';
        }

        function resolveStockByKeyword(raw) {
            var keyword = String(raw || '').trim();
            if (!keyword) {
                return;
            }
            fetch('/api/stock/resolve?q=' + encodeURIComponent(keyword))
                .then(function(resp) { return resp.json(); })
                .then(function(data) {
                    if (!data || !data.ok || !data.symbol) {
                        return;
                    }
                    if (window.selectStock) {
                        window.selectStock(
                            data.symbol,
                            data.name || data.symbol,
                            Number(data.last_close || 0),
                        );
                    }
                })
                .catch(function() {});
        }

        function syncStockByInput() {
            var searchInput = document.getElementById('stock-search');
            var symbolInput = document.getElementById('selected-symbol');
            var nameInput = document.getElementById('selected-stock-name');
            if (!searchInput || !symbolInput || !nameInput) {
                return;
            }
            var raw = searchInput.value || '';
            if (!String(raw).trim()) {
                symbolInput.value = '';
                nameInput.value = '';
                resetSpeedDial();
                return;
            }
            var symbol = parseSymbolFromInputValue(raw);
            if (!symbol) {
                resolveStockByKeyword(raw);
                return;
            }
            if (symbol.indexOf('.') === -1) {
                resolveStockByKeyword(raw);
                return;
            }
            var name = raw.split('(')[0].trim() || symbol;
            symbolInput.value = symbol;
            nameInput.value = name;
            fetchAndRenderSpeedDial(symbol);
        }

        var stockInputTimer = null;
        window.onStockInputChange = function(raw) {
            if (stockInputTimer) {
                clearTimeout(stockInputTimer);
            }
            stockInputTimer = setTimeout(function() {
                var value = String(raw || '').trim();
                if (value.length < 2) {
                    var symbolInput = document.getElementById('selected-symbol');
                    var nameInput = document.getElementById('selected-stock-name');
                    if (symbolInput) {
                        symbolInput.value = '';
                    }
                    if (nameInput) {
                        nameInput.value = '';
                    }
                    resetSpeedDial();
                    return;
                }
                syncStockByInput();
            }, 250);
        };

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
                    refreshOrderEstimate();
                }
                
                // 更新 Speed Dial 价格
                if (lastClose > 0) {
                    updateSpeedDial(lastClose);
                } else if (symbol) {
                    fetchAndRenderSpeedDial(symbol);
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
                    var displayNum = levels[index][0];
                    var pct = levels[index][1];
                    var price = lastClose * (1 + pct / 100);
                    var pctEl = btn.querySelector('.speed-dial-pct');
                    if (pctEl) {
                        pctEl.textContent = (displayNum > 0 ? '+' : '') + String(displayNum) + '%';
                    }
                    var priceDiv = btn.querySelector('.speed-dial-price');
                    if (priceDiv) {
                        priceDiv.textContent = price.toFixed(2);
                        priceDiv.classList.remove('hidden');
                        priceDiv.classList.add(
                            'absolute',
                            'right-2',
                            'bottom-1',
                            'text-[11px]',
                            'font-medium',
                            'text-gray-400',
                            'leading-none',
                        );
                    }
                    btn.disabled = false;
                    btn.onclick = function() {
                        if (window.applySpeedDialPrice) {
                            window.applySpeedDialPrice(price);
                        }
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
        function getOrderMode() {
            var selected = document.querySelector('input[name="order-mode"]:checked');
            return selected ? selected.value : 'amount';
        }

        function formatNumber(value, digits) {
            return Number(value || 0).toLocaleString('zh-CN', {
                minimumFractionDigits: 0,
                maximumFractionDigits: digits,
            });
        }

        function calcSharesByAmountWan(amountWan, price) {
            if (price <= 0 || amountWan <= 0) {
                return 0;
            }
            return Math.floor((amountWan * 10000) / price / 100) * 100;
        }

        function calcSharesByHands(hands) {
            if (hands <= 0) {
                return 0;
            }
            return Math.floor(hands) * 100;
        }

        function getAvailableCash() {
            var cashInput = document.getElementById('available-cash');
            if (!cashInput || !cashInput.value) {
                return 0;
            }
            var cash = parseFloat(cashInput.value);
            return Number.isFinite(cash) ? cash : 0;
        }

        window.refreshOrderEstimate = function() {
            var price = getCurrentPrice();
            var mode = getOrderMode();
            var orderValueInput = document.getElementById('order-value');
            var estimate = document.getElementById('order-estimate');
            var sharesInput = document.getElementById('est-shares');
            if (!orderValueInput || !estimate || !sharesInput) {
                return;
            }
            var value = parseFloat(orderValueInput.value || '0');
            if (!(value > 0) || !(price > 0)) {
                estimate.textContent = '--';
                sharesInput.value = '';
                return;
            }
            if (mode === 'amount') {
                var sharesByAmount = calcSharesByAmountWan(value, price);
                var hands = sharesByAmount / 100;
                sharesInput.value = String(sharesByAmount);
                estimate.textContent = formatNumber(hands, 2) + ' 手';
                return;
            }
            var sharesByHands = calcSharesByHands(value);
            var amountWan = (sharesByHands * price) / 10000;
            sharesInput.value = String(sharesByHands);
            estimate.textContent = formatNumber(amountWan, 2) + ' 万元';
        };

        window.onAmountChange = function() {
            refreshOrderEstimate();
        };
        window.onSharesChange = function() {
            refreshOrderEstimate();
        };

        window.onOrderModeChange = function() {
            var mode = getOrderMode();
            var valueLabel = document.getElementById('order-value-label');
            var valueUnit = document.getElementById('order-value-unit');
            var estimateLabel = document.getElementById('order-estimate-label');
            var valueInput = document.getElementById('order-value');
            var estimate = document.getElementById('order-estimate');
            if (
                !valueLabel || !valueUnit || !estimateLabel ||
                !valueInput || !estimate
            ) {
                return;
            }
            if (mode === 'amount') {
                valueLabel.textContent = '下单金额';
                valueUnit.textContent = '万元';
                estimateLabel.textContent = '预估手数：';
                valueInput.placeholder = '请输入金额';
                valueInput.step = '0.1';
            } else {
                valueLabel.textContent = '下单数量';
                valueUnit.textContent = '手';
                estimateLabel.textContent = '预估金额：';
                valueInput.placeholder = '请输入手数';
                valueInput.step = '1';
            }
            if (!valueInput.value) {
                estimate.textContent = '--';
            }
            refreshOrderEstimate();
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
            refreshOrderEstimate();
        };
        // 设置仓位比例
        window.setPositionRatio = function(ratio) {
            var mode = getOrderMode();
            var price = getCurrentPrice();
            var orderValueInput = document.getElementById('order-value');
            var availableCash = getAvailableCash();
            if (!orderValueInput || !(ratio > 0) || !(availableCash > 0)) {
                return;
            }
            if (mode === 'amount') {
                var amountWan = (availableCash * ratio) / 10000;
                orderValueInput.value = amountWan.toFixed(2);
                refreshOrderEstimate();
                return;
            }
            if (!(price > 0)) {
                orderValueInput.value = '';
                refreshOrderEstimate();
                return;
            }
            var shares = Math.floor((availableCash * ratio) / price / 100) * 100;
            var hands = shares / 100;
            orderValueInput.value = String(Math.max(hands, 0));
            refreshOrderEstimate();
        };
        // 点击页面其他地方时隐藏下拉列表
        document.addEventListener('click', function(e) {
            var searchContainer = document.getElementById('stock-search');
            var suggestions = document.getElementById('stock-suggestions');
            if (!searchContainer || !suggestions) {
                return;
            }
            var isOutsideSearch = !searchContainer.contains(e.target);
            var isOutsideSuggestions = !suggestions.contains(e.target);
            if (isOutsideSearch && isOutsideSuggestions) {
                suggestions.innerHTML = '';
            }
        });

        function attachStockInputListeners() {
            var searchInput = document.getElementById('stock-search');
            if (!searchInput) {
                return;
            }
            searchInput.addEventListener('change', syncStockByInput);
            searchInput.addEventListener('blur', syncStockByInput);
            searchInput.addEventListener('keyup', function(e) {
                if (e && e.key === 'Enter') {
                    syncStockByInput();
                }
            });
        }

        onOrderModeChange();
        attachStockInputListeners();

        document.addEventListener('DOMContentLoaded', function() {
            onOrderModeChange();
            attachStockInputListeners();
        });
    """)
    return create_main_page(
        # JavaScript
        stock_selection_script,
        # 第1行：顶部账号资金
        AccountInfo(asset),
        # 第2行：下单表单 + Speed Dial
        Div(
            Div(OrderForm(asset.get("cash", 0) if asset else 0), cls="w-1/3 pr-4"),
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
