# 分析导航功能 - 前端组件设计文档

## 1. 概述

本文档描述分析导航功能的前端组件设计，包括页面布局、K线图组件、板块管理组件等。

## 2. 技术选型

### 2.1 K线图库选择

选用 **Lightweight Charts** (TradingView出品)

**理由：**
- ✅ 轻量级（~40KB gzipped）
- ✅ 性能优秀，支持大量数据点
- ✅ 专为金融数据设计
- ✅ 支持多图联动
- ✅ 支持自定义技术指标
- ✅ 移动端友好

**替代方案：** ECharts（功能更丰富但体积更大）

### 2.2 组件库

继续使用 **MonsterUI** + **FastHTML**

## 3. 页面布局

### 3.1 分析导航主页面

```
┌─────────────────────────────────────────────────────────────────┐
│  Header (保持不变)                                               │
├──────────┬──────────────────────────────────────────────────────┤
│          │  工具栏                                                │
│  左侧面板  │  ┌─────────────┬─────────────┬─────────────┐        │
│          │  │  个股分析    │  板块分析    │  指数分析    │        │
│  ┌────┐  │  └─────────────┴─────────────┴─────────────┘        │
│  │板块 │  │                                                     │
│  │列表 │  │  主内容区                                            │
│  ├────┤  │  ┌───────────────────────────────────────────────┐  │
│  │个股 │  │  │                                              │  │
│  │列表 │  │  │           K线图显示区域                        │  │
│  │    │  │  │                                              │  │
│  │    │  │  │                                              │  │
│  └────┘  │  └───────────────────────────────────────────────┘  │
│          │                                                     │
│  ┌────┐  │  周期选择: [日线] [周线] [月线]                       │
│  │操作 │  │  均线: [MA5] [MA10] [MA20] [MA60]                   │
│  │按钮 │  │  对比: [上证指数 ▼]                                 │
│  └────┘  │                                                     │
│          │                                                     │
└──────────┴──────────────────────────────────────────────────────┘
```

### 3.2 页面路由

```
/analysis                    # 分析导航首页（默认显示个股分析）
/analysis/stock/{symbol}     # 个股分析
/analysis/sector/{id}        # 板块分析
/analysis/index/{symbol}     # 指数分析
/analysis/sectors            # 板块管理页面
```

## 4. 组件设计

### 4.1 K线图组件 (KlineChart)

**功能：**
- 显示K线图（日/周/月）
- 显示成交量柱状图
- 叠加均线（MA5/10/20/60）
- 支持指数对比
- 支持多图联动

**Props：**
```python
@dataclass
class KlineChartProps:
    symbol: str                    # 股票/指数代码
    name: str                      # 名称
    freq: str = "day"              # 周期: day/week/month
    data: list[KlineData]          # K线数据
    ma_periods: list[int] = None   # 均线周期 [5, 10, 20, 60]
    compare_data: list[KlineData] = None  # 对比数据
    compare_symbol: str = ""       # 对比代码
    height: int = 500              # 图表高度
    on_freq_change: Callable = None  # 周期切换回调
```

**数据结构：**
```python
@dataclass
class KlineData:
    dt: str            # 日期 YYYY-MM-DD
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    ma5: float = None
    ma10: float = None
    ma20: float = None
    ma60: float = None
```

**实现要点：**
```javascript
// Lightweight Charts 配置
const chart = LightweightCharts.createChart(container, {
    layout: {
        background: { color: '#ffffff' },
        textColor: '#333',
    },
    grid: {
        vertLines: { color: '#f0f0f0' },
        horzLines: { color: '#f0f0f0' },
    },
    crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal,
    },
    rightPriceScale: {
        borderColor: '#cccccc',
    },
    timeScale: {
        borderColor: '#cccccc',
    },
});

// K线系列
const candlestickSeries = chart.addCandlestickSeries({
    upColor: '#ef4444',      // 红色（上涨）
    downColor: '#22c55e',    // 绿色（下跌）
    borderUpColor: '#ef4444',
    borderDownColor: '#22c55e',
    wickUpColor: '#ef4444',
    wickDownColor: '#22c55e',
});

// 成交量系列
const volumeSeries = chart.addHistogramSeries({
    color: '#26a69a',
    priceFormat: { type: 'volume' },
    priceScaleId: '',
    scaleMargins: {
        top: 0.8,
        bottom: 0,
    },
});

// 均线系列
const maSeries = chart.addLineSeries({
    color: '#2962FF',
    lineWidth: 1,
    title: 'MA5',
});
```

### 4.2 板块列表组件 (SectorList)

**功能：**
- 显示板块列表（自定义/行业/概念）
- 支持搜索过滤
- 支持分类筛选
- 点击选中板块

**Props：**
```python
@dataclass
class SectorListProps:
    sectors: list[Sector]          # 板块列表
    selected_id: str = ""          # 当前选中ID
    filter_type: str = "all"       # 过滤类型: all/custom/industry/concept
    search_query: str = ""         # 搜索关键词
    on_select: Callable = None     # 选择回调
    on_create: Callable = None     # 创建回调
    on_edit: Callable = None       # 编辑回调
    on_delete: Callable = None     # 删除回调
```

**UI结构：**
```html
<div class="sector-list">
    <div class="toolbar">
        <input type="text" placeholder="搜索板块..." />
        <select>
            <option>全部</option>
            <option>自定义</option>
            <option>行业</option>
            <option>概念</option>
        </select>
        <button>+ 新建板块</button>
    </div>
    <div class="list">
        <div class="sector-item active">
            <span class="name">新能源板块</span>
            <span class="count">10只</span>
        </div>
        ...
    </div>
</div>
```

### 4.3 个股列表组件 (StockList)

**功能：**
- 显示板块内个股列表
- 显示股票名称和代码
- 支持删除个股
- 支持导入个股

**Props：**
```python
@dataclass
class StockListProps:
    sector_id: str                 # 板块ID
    stocks: list[SectorStock]      # 个股列表
    selected_symbol: str = ""      # 当前选中代码
    on_select: Callable = None     # 选择回调
    on_remove: Callable = None     # 删除回调
    on_import: Callable = None     # 导入回调
```

### 4.4 周期选择器组件 (PeriodSelector)

**功能：**
- 切换日/周/月周期

**Props：**
```python
@dataclass
class PeriodSelectorProps:
    current: str = "day"           # 当前周期
    options: list[str] = None      # 选项 ["day", "week", "month"]
    on_change: Callable = None     # 切换回调
```

### 4.5 均线配置组件 (MAConfig)

**功能：**
- 选择显示的均线
- 配置均线周期

**Props：**
```python
@dataclass
class MAConfigProps:
    periods: list[int] = None      # 当前周期 [5, 10, 20, 60]
    available: list[int] = None    # 可用周期 [5, 10, 20, 30, 60, 120, 250]
    on_change: Callable = None     # 变更回调
```

### 4.6 对比选择器组件 (CompareSelector)

**功能：**
- 选择对比的指数
- 支持取消对比

**Props：**
```python
@dataclass
class CompareSelectorProps:
    indices: list[Index]           # 可选指数列表
    selected: str = ""             # 当前选中
    on_change: Callable = None     # 变更回调
```

### 4.7 多股同屏组件 (MultiStockView)

**功能：**
- 网格布局显示多只股票
- 每格显示迷你K线
- 支持2/3/4列切换

**Props：**
```python
@dataclass
class MultiStockViewProps:
    stocks: list[Stock]            # 股票列表
    freq: str = "day"              # 周期
    columns: int = 2               # 列数
    on_stock_click: Callable = None  # 点击回调
```

## 5. 页面组件

### 5.1 分析主页面 (AnalysisPage)

**布局结构：**
```python
def AnalysisPage():
    return Div(
        # 顶部工具栏
        ToolBar(
            PeriodSelector(),
            MAConfig(),
            CompareSelector(),
        ),
        # 主体区域
        Div(
            # 左侧面板
            Div(
                SectorList(),
                StockList(),
                cls="left-panel w-64",
            ),
            # 右侧主内容
            Div(
                KlineChart(),
                cls="main-content flex-1",
            ),
            cls="flex h-full",
        ),
        cls="analysis-page h-full",
    )
```

### 5.2 板块管理页面 (SectorManagePage)

**功能：**
- 板块CRUD
- 成分股管理
- 文件导入

**布局：**
```
┌─────────────────────────────────────────────────────┐
│  板块管理                                    [+新建] │
├─────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────────────────────┐ │
│  │              │  │  板块详情                      │ │
│  │  板块列表     │  │  ───────────────────────────  │ │
│  │              │  │  名称: [新能源板块        ]    │ │
│  │  ○ 新能源    │  │  描述: [新能源相关股票    ]    │ │
│  │  ○ 金融      │  │                              │ │
│  │  ○ 消费      │  │  成分股 (10只)          [导入] │ │
│  │              │  │  ┌────────────────────────┐  │ │
│  │              │  │  │ 000001.SZ 平安银行  [×]│  │ │
│  │              │  │  │ 600519.SH 贵州茅台  [×]│  │ │
│  │              │  │  │ ...                    │  │ │
│  │              │  │  └────────────────────────┘  │ │
│  │              │  │                              │ │
│  │              │  │  [保存] [删除]               │ │
│  └──────────────┘  └──────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

## 6. 状态管理

使用简单的状态对象管理页面状态：

```python
@dataclass
class AnalysisState:
    # 当前视图
    view_type: str = "stock"       # stock/sector/index
    
    # 选中项
    selected_sector_id: str = ""
    selected_stock_symbol: str = ""
    selected_index_symbol: str = ""
    
    # K线配置
    freq: str = "day"              # day/week/month
    ma_periods: list[int] = None
    compare_symbol: str = ""
    
    # 数据
    sectors: list[Sector] = None
    stocks: list[SectorStock] = None
    kline_data: list[KlineData] = None
    
    # UI状态
    loading: bool = False
    error: str = ""
```

## 7. HTMX交互

### 7.1 页面加载

```python
# 加载板块列表
@rt("/api/sectors")
def get_sectors():
    sectors = sector_dal.list_sectors()
    return SectorList(sectors=sectors)

# 加载成分股
@rt("/api/sectors/{sector_id}/stocks")
def get_sector_stocks(sector_id: str):
    stocks = sector_dal.get_sector_stocks(sector_id)
    return StockList(stocks=stocks)
```

### 7.2 K线数据加载

```python
# 加载K线数据（返回JSON给前端JS）
@rt("/api/kline/{symbol}")
def get_kline(symbol: str, freq: str = "day"):
    data = kline_service.get_data(symbol, freq)
    return {"data": data}
```

### 7.3 表单提交

```python
# 创建板块
@rt("/api/sectors", methods=["POST"])
def create_sector(req):
    form = req.form()
    sector = Sector(name=form["name"], ...)
    sector_dal.create(sector)
    return Redirect("/analysis/sectors")
```

## 8. 响应式设计

### 8.1 断点设计

```css
/* 大屏：三栏布局 */
@media (min-width: 1280px) {
    .left-panel { width: 280px; }
    .main-content { flex: 1; }
}

/* 中屏：两栏布局 */
@media (min-width: 768px) and (max-width: 1279px) {
    .left-panel { width: 240px; }
}

/* 小屏：单栏，侧边栏可收起 */
@media (max-width: 767px) {
    .left-panel { 
        position: fixed;
        transform: translateX(-100%);
    }
    .left-panel.open {
        transform: translateX(0);
    }
}
```

## 9. 性能优化

### 9.1 数据加载

- K线数据分页加载（每次加载200条）
- 懒加载：滚动到可视区域再加载图表
- 防抖：搜索输入防抖300ms

### 9.2 图表优化

- 数据点超过1000时启用数据抽样
- 隐藏不可见图表以节省内存
- 使用 `requestAnimationFrame` 优化动画

### 9.3 缓存策略

- 板块列表缓存5分钟
- K线数据按symbol+freq缓存
- 使用 localStorage 缓存用户配置（均线周期等）

## 10. 文件结构

```
pyqmt/web/
├── pages/
│   ├── analysis.py          # 分析主页面
│   ├── sector_manage.py     # 板块管理页面
│   └── ...
├── components/
│   ├── kline_chart.py       # K线图组件
│   ├── sector_list.py       # 板块列表
│   ├── stock_list.py        # 个股列表
│   ├── period_selector.py   # 周期选择器
│   ├── ma_config.py         # 均线配置
│   └── compare_selector.py  # 对比选择器
├── static/
│   └── js/
│       └── lightweight-charts.js  # K线图库
└── ...
```
