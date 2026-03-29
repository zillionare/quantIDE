"""K线图组件 - 基于 Lightweight Charts"""

from fasthtml.common import *


class KlineChart:
    """K线图组件

    使用 Lightweight Charts 库渲染K线图，支持多周期、均线、成交量。
    """

    def __init__(
        self,
        chart_id: str = "kline-chart",
        height: int = 500,
        symbol: str = "",
        name: str = "",
        freq: str = "day",
    ):
        self.chart_id = chart_id
        self.js_id = chart_id.replace("-", "_")  # JavaScript 变量名不能包含 -
        self.height = height
        self.symbol = symbol
        self.name = name
        self.freq = freq

    def _get_chart_js(self) -> str:
        """生成图表初始化 JavaScript 代码"""
        return f"""
        (function() {{
            const chartElement = document.getElementById('{self.chart_id}');
            if (!chartElement) return;

            // 清理旧的图表
            if (window.{self.js_id}_instance) {{
                window.{self.js_id}_instance.remove();
                window.{self.js_id}_instance = null;
            }}

            // 创建图表
            const chart = LightweightCharts.createChart(chartElement, {{
                width: chartElement.clientWidth,
                height: {self.height},
                layout: {{
                    background: {{ color: '#ffffff' }},
                    textColor: '#333',
                }},
                grid: {{
                    vertLines: {{ color: '#e0e0e0' }},
                    horzLines: {{ color: '#e0e0e0' }},
                }},
                crosshair: {{
                    mode: LightweightCharts.CrosshairMode.Normal,
                }},
                rightPriceScale: {{
                    borderColor: '#e0e0e0',
                }},
                timeScale: {{
                    borderColor: '#e0e0e0',
                    timeVisible: true,
                }},
            }});

            window.{self.js_id}_instance = chart;

            // 创建K线系列
            const candlestickSeries = chart.addCandlestickSeries({{
                upColor: '#ef4444',
                downColor: '#22c55e',
                borderUpColor: '#ef4444',
                borderDownColor: '#22c55e',
                wickUpColor: '#ef4444',
                wickDownColor: '#22c55e',
            }});

            window.{self.js_id}_candlestick = candlestickSeries;

            // 创建成交量系列
            const volumeSeries = chart.addHistogramSeries({{
                color: '#26a69a',
                priceFormat: {{
                    type: 'volume',
                }},
                priceScaleId: '',
                scaleMargins: {{
                    top: 0.8,
                    bottom: 0,
                }},
            }});

            window.{self.js_id}_volume = volumeSeries;

            // 响应窗口大小变化
            window.addEventListener('resize', () => {{
                chart.applyOptions({{
                    width: chartElement.clientWidth,
                }});
            }});
        }})();
        """

    def _get_update_js(self) -> str:
        """生成数据更新 JavaScript 代码"""
        return f"""
        function updateKlineData_{self.js_id}(data, maData) {{
            if (!window.{self.js_id}_candlestick) return;

            const candleData = data.map(item => ({{
                time: item.dt,
                open: item.open,
                high: item.high,
                low: item.low,
                close: item.close,
            }}));

            const volumeData = data.map(item => ({{
                time: item.dt,
                value: item.volume,
                color: item.close >= item.open ? '#ef4444' : '#22c55e',
            }}));

            window.{self.js_id}_candlestick.setData(candleData);
            window.{self.js_id}_volume.setData(volumeData);

            // 更新均线
            if (maData) {{
                Object.keys(maData).forEach(maKey => {{
                    const maSeries = window['{self.js_id}_ma_' + maKey];
                    if (maSeries) {{
                        maSeries.setData(maData[maKey]);
                    }}
                }});
            }}

            // 调整时间范围
            if (window.{self.js_id}_instance && candleData.length > 0) {{
                window.{self.js_id}_instance.timeScale().fitContent();
            }}
        }}

        function addMA_{self.js_id}(period, data) {{
            if (!window.{self.js_id}_instance) return;

            const maSeries = window.{self.js_id}_instance.addLineSeries({{
                color: getMAColor(period),
                lineWidth: 1,
                title: 'MA' + period,
            }});

            window['{self.js_id}_ma_' + period] = maSeries;

            const maData = data.map(item => ({{
                time: item.dt,
                value: item['ma' + period],
            }})).filter(item => item.value !== null && item.value !== undefined);

            maSeries.setData(maData);
        }}

        function getMAColor(period) {{
            const colors = {{
                5: '#ff6d00',
                10: '#2962ff',
                20: '#00c853',
                60: '#aa00ff',
            }};
            return colors[period] || '#999999';
        }}

        function setSymbol_{self.js_id}(symbol, name) {{
            const titleEl = document.getElementById('{self.chart_id}-title');
            if (titleEl) {{
                titleEl.textContent = name + ' (' + symbol + ')';
            }}
        }}
        """

    def render(self) -> FT:
        """渲染组件"""
        # Lightweight Charts CDN
        chart_lib = Script(src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js")

        # 图表容器
        chart_container = Div(
            Div(
                f"{self.name} ({self.symbol})" if self.symbol else "请选择股票",
                id=f"{self.chart_id}-title",
                cls="text-lg font-semibold mb-2",
            ),
            Div(
                id=self.chart_id,
                cls="w-full bg-white rounded-lg shadow",
                style=f"height: {self.height}px;",
            ),
            cls="kline-chart-container",
        )

        # 初始化脚本
        init_script = Script(self._get_chart_js())
        update_script = Script(self._get_update_js())

        return Div(
            chart_lib,
            chart_container,
            init_script,
            update_script,
            cls="kline-chart-wrapper",
        )

    @staticmethod
    def freq_buttons(chart_id: str, current_freq: str = "day") -> FT:
        """渲染周期切换按钮"""
        freqs = [
            ("day", "日线"),
            ("week", "周线"),
            ("month", "月线"),
        ]

        buttons = []
        for freq, label in freqs:
            is_active = freq == current_freq
            btn_cls = (
                "px-3 py-1 text-sm rounded "
                + ("bg-blue-600 text-white" if is_active else "bg-gray-200 text-gray-700 hover:bg-gray-300")
            )
            js_chart_id = chart_id.replace("-", "_")
            buttons.append(
                Button(
                    label,
                    cls=btn_cls,
                    data_freq=freq,
                    onclick=f"switchFreq_{js_chart_id}('{freq}')",
                )
            )

        return Div(*buttons, cls="flex gap-2")

    @staticmethod
    def ma_buttons(chart_id: str, ma_periods: list[int] = None) -> FT:
        """渲染均线切换按钮"""
        if ma_periods is None:
            ma_periods = [5, 10, 20, 60]

        buttons = []
        js_chart_id = chart_id.replace("-", "_")
        for period in ma_periods:
            btn_cls = (
                "px-2 py-1 text-xs rounded border "
                "border-gray-300 text-gray-600 hover:bg-gray-100"
            )
            buttons.append(
                Button(
                    f"MA{period}",
                    cls=btn_cls,
                    onclick=f"toggleMA_{js_chart_id}({period})",
                )
            )

        return Div(*buttons, cls="flex gap-1")
