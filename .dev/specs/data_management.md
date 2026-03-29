# PyQMT 数据管理模块 UI 需求与设计文档

## 1. 需求概述

根据用户提供的设计原型图，本次需要在 `pyqmt` 主体工程中实现「数据管理」模块的相关页面。
该模块主要包括四个子页面功能：
1. **交易日历 (/data/calendar)**
2. **行情数据 (/data/market)**
3. **股票列表 (/data/stocks)**
4. **数据库管理 (/data/db)**

> **注意**：
> 1. 本次开发需要遵循 `pyqmt` 现有的技术栈：**FastHTML + MonsterUI + TailwindCSS**。
> 2. 原型图中的红色下划线 Tab 风格（主色调）需要在项目中还原。
> 3. 需要确保 `pyqmt` 侧边栏（`SIDEBAR_MENUS`）的链接能够正确路由到这些页面。

---

## 2. 界面结构与交互规范

### 2.1 整体布局
复用项目现有的 `MainLayout`，左侧保留 `SIDEBAR_MENUS` 中的导航菜单，右侧为各页面的主内容区。

### 2.2 顶部 Tab 导航风格
由于各子页面内部均包含二级的 Tab 导航（如：概要、日历、手动更新），根据 PyQMT 的设计规范与原型图，Tab 的 HTML 结构设计如下：
- **容器**：`Div(cls="flex border-b mb-4")`
- **默认（未选中）状态**：`A(title, href=url, cls="px-4 py-2 font-medium text-gray-500 hover:text-gray-700")`
- **激活（选中）状态**：`A(title, href=url, cls="px-4 py-2 font-medium text-red-600 border-b-2 border-red-600")`

---

## 3. 各子页面功能详细设计

### 3.1 交易日历 (`/data/calendar`)
展示交易日历信息，包含三个子 Tab：
- **概要**：显示日历的开始日期、结束日期，以及底层 `calendar.parquet` 的文件绝对路径。
- **日历**：以月度网格（Grid）的形式可视化展示交易日历（红字标注“休市”，其余为交易日）。上方提供月份切换控件。
- **手动更新**：提供「执行日历更新」按钮，点击后触发后端更新逻辑。

### 3.2 行情数据 (`/data/market`)
管理日线等行情数据，包含四个子 Tab：
- **概要**：显示“日线数据状态”，包括数据起始日期、结束日期（若非最新需高亮，如红色标注）、数据天数、记录总数。下方提供“数据覆盖范围”的可视化散点图（可简化为年/月状态指示矩阵或暂用占位图）。
- **数据校验**：提供表单，允许输入“资产(逗号分隔)”、“开始年份”、“结束年份”，并点击“手动校验”检查 ST、涨跌停数据的缺失情况。
- **手动补全**：提供表单，指定开始日期和结束日期，执行“手动更新”操作。
- **浏览**：提供数据浏览表格（Table），顶部支持按证券代码、起始日期、结束日期筛选，表格列包括：日期、asset、open、high、low、close、volume、amount、adjust、st(复选框)、up_limit、down_limit。

### 3.3 股票列表 (`/data/stocks`)
管理基础证券列表，包含两个子 Tab：
- **概要**：显示股票总数、最后更新日期，以及底层 `stock_list.parquet` 的文件绝对路径。
- **查询**：提供关键字输入框（支持停顿后自动搜索），下方展示股票数据表格（字段：股票代码、公司名称、拼音、上市日期、退市日期、symbol）。
- **手动更新**：提供「立即更新」按钮，触发列表更新。

### 3.4 数据库管理 (`/data/db`)
提供底层数据库（SQLite / Parquet 元数据）的管理界面。包含两个子 Tab：
- **数据视图**：顶部下拉框选择数据表（如 `failed_tasks`），支持分页、删除选中行、保存修改。下方为数据表格展示区。
- **元数据视图**：展示表的结构信息。

---

## 4. 技术实施方案

### 4.1 路由与应用注册
为了避免在 `pyqmt/app.py` 中堆积过多路由，计划在 `pyqmt/web/pages/` 下创建新的独立路由模块：
- `pyqmt/web/pages/data_calendar.py`
- `pyqmt/web/pages/data_market.py`
- `pyqmt/web/pages/data_stocks.py`
- `pyqmt/web/pages/data_db.py`

在 `pyqmt/app.py` 中引入并挂载（Mount/Route）：
```python
from pyqmt.web.pages.data_calendar import data_calendar_app
from pyqmt.web.pages.data_market import data_market_app
from pyqmt.web.pages.data_stocks import data_stocks_app
from pyqmt.web.pages.data_db import data_db_app

# ... 现有路由中增加
Mount("/data/calendar", data_calendar_app),
Mount("/data/market", data_market_app),
Mount("/data/stocks", data_stocks_app),
Mount("/data/db", data_db_app),
```

### 4.2 数据层对接
- **日历数据**：对接 `pyqmt.data.models.calendar.calendar` 实例。
- **股票数据**：对接 `pyqmt.data.models.stocks.StockList` 实例。
- **行情数据**：对接 `pyqmt.data.models.daily_bars.daily_bars` 等存储接口。
- **UI 图标**：使用 `monsterui.all` 提供的标准 `UkIcon`（如 `calendar`, `bar-chart`, `database` 等）。

---
**后续实施计划：**
待用户确认本 Spec 设计无误后，将按照上述规范依次创建文件并实现界面与交互。
