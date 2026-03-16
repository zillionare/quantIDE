# 分析导航功能 - 数据层设计文档

## 1. 概述

本文档描述分析导航功能的数据层设计，包括板块管理、指数管理和K线数据重采样。

## 2. 数据表设计

### 2.1 板块表 (sectors)

存储用户自定义板块和行业板块（从tushare同步）。

```sql
CREATE TABLE sectors (
    id TEXT PRIMARY KEY,                    -- 板块代码（用户自定义或tushare代码）
    name TEXT NOT NULL,                     -- 板块名称
    sector_type TEXT NOT NULL,              -- 类型：'custom'(用户自定义) / 'industry'(行业) / 'concept'(概念)
    source TEXT,                            -- 来源：'user' / 'tushare'
    description TEXT,                       -- 描述
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sectors_type ON sectors(sector_type);
CREATE INDEX idx_sectors_source ON sectors(source);
```

### 2.2 板块成分股表 (sector\_stocks)

存储板块与个股的关联关系。

```sql
CREATE TABLE sector_stocks (
    sector_id TEXT,
    symbol TEXT,                            -- 股票代码，如 '000001.SZ'
    name TEXT,                              -- 股票名称（缓存）
    weight REAL,                            -- 权重（可选，用于加权计算）
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sector_id, symbol),
    FOREIGN KEY (sector_id) REFERENCES sectors(id) ON DELETE CASCADE
);

CREATE INDEX idx_sector_stocks_sector ON sector_stocks(sector_id);
CREATE INDEX idx_sector_stocks_symbol ON sector_stocks(symbol);
```

### 2.3 指数列表表 (indices)

存储指数基本信息（从tushare同步）。

```sql
CREATE TABLE indices (
    symbol TEXT PRIMARY KEY,                -- 指数代码，如 '000001.SH'
    name TEXT NOT NULL,                     -- 指数名称
    index_type TEXT,                        -- 类型：'market'(市场指数) / 'industry'(行业指数) / 'concept'(概念指数)
    category TEXT,                          -- 分类：如 '上证系列' / '深证系列' / '中证系列'
    publisher TEXT,                         -- 发布机构
    base_date DATE,                         -- 基准日期
    base_point REAL,                        -- 基准点数
    list_date DATE,                         -- 上市日期
    description TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_indices_type ON indices(index_type);
CREATE INDEX idx_indices_category ON indices(category);
```

### 2.4 板块行情表 (sector\_bars)

存储板块日线行情数据。

```sql
CREATE TABLE sector_bars (
    sector_id TEXT,
    dt DATE,                                -- 交易日期
    open REAL,                              -- 开盘价
    high REAL,                              -- 最高价
    low REAL,                               -- 最低价
    close REAL,                             -- 收盘价
    volume INTEGER,                         -- 成交量（股）
    amount REAL,                            -- 成交额（元）
    PRIMARY KEY (sector_id, dt),
    FOREIGN KEY (sector_id) REFERENCES sectors(id) ON DELETE CASCADE
);

CREATE INDEX idx_sector_bars_date ON sector_bars(dt);
```

### 2.5 指数行情表 (index\_bars)

存储指数日线行情数据。

```sql
CREATE TABLE index_bars (
    symbol TEXT,                            -- 指数代码
    dt DATE,                                -- 交易日期
    open REAL,                              -- 开盘价
    high REAL,                              -- 最高价
    low REAL,                               -- 最低价
    close REAL,                             -- 收盘价
    volume INTEGER,                         -- 成交量（股）
    amount REAL,                            -- 成交额（元）
    PRIMARY KEY (symbol, dt),
    FOREIGN KEY (symbol) REFERENCES indices(symbol) ON DELETE CASCADE
);

CREATE INDEX idx_index_bars_symbol ON index_bars(symbol);
CREATE INDEX idx_index_bars_date ON index_bars(dt);
```

## 3. 数据模型

### 3.1 Sector 模型

```python
@dataclass
class Sector:
    id: str
    name: str
    sector_type: str  # 'custom', 'industry', 'concept'
    source: str       # 'user', 'tushare'
    description: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    __pk__ = "id"
```

### 3.2 SectorStock 模型

```python
@dataclass
class SectorStock:
    sector_id: str
    symbol: str
    name: str = ""
    weight: float = 0.0
    added_at: datetime = field(default_factory=datetime.now)
    
    __pk__ = ("sector_id", "symbol")
```

### 3.3 Index 模型

```python
@dataclass
class Index:
    symbol: str
    name: str
    index_type: str   # 'market', 'industry', 'concept'
    category: str = ""
    publisher: str = ""
    base_date: date | None = None
    base_point: float = 0.0
    list_date: date | None = None
    description: str = ""
    updated_at: datetime = field(default_factory=datetime.now)
    
    __pk__ = "symbol"
```

### 3.4 Bar 模型（复用现有）

复用现有的 `DailyBar` 模型或创建通用的 `Bar` 模型：

```python
@dataclass
class Bar:
    symbol: str      # 股票/指数/板块代码
    dt: date         # 日期
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
```

## 4. 数据访问层 (DAL)

### 4.1 SectorDAL

```python
class SectorDAL:
    """板块数据访问层"""
    
    def __init__(self, db: Database):
        self.db = db
    
    def create_sector(self, sector: Sector) -> Sector:
        """创建板块"""
        
    def get_sector(self, sector_id: str) -> Sector | None:
        """获取板块"""
        
    def list_sectors(self, sector_type: str | None = None) -> list[Sector]:
        """列出板块"""
        
    def update_sector(self, sector: Sector) -> Sector:
        """更新板块"""
        
    def delete_sector(self, sector_id: str) -> bool:
        """删除板块"""
        
    def add_stock_to_sector(self, sector_id: str, symbol: str, name: str = "") -> bool:
        """添加股票到板块"""
        
    def remove_stock_from_sector(self, sector_id: str, symbol: str) -> bool:
        """从板块移除股票"""
        
    def get_sector_stocks(self, sector_id: str) -> list[SectorStock]:
        """获取板块成分股"""
        
    def import_stocks_from_file(self, sector_id: str, file_path: str) -> tuple[int, int]:
        """从文件导入股票列表，返回 (成功数, 失败数)"""
```

### 4.2 IndexDAL

```python
class IndexDAL:
    """指数数据访问层"""
    
    def __init__(self, db: Database):
        self.db = db
    
    def create_index(self, index: Index) -> Index:
        """创建指数记录"""
        
    def get_index(self, symbol: str) -> Index | None:
        """获取指数"""
        
    def list_indices(self, index_type: str | None = None) -> list[Index]:
        """列出指数"""
        
    def update_index(self, index: Index) -> Index:
        """更新指数"""
        
    def delete_index(self, symbol: str) -> bool:
        """删除指数"""
```

### 4.3 BarDAL

```python
class BarDAL:
    """行情数据访问层"""
    
    def __init__(self, db: Database):
        self.db = db
    
    def save_sector_bars(self, bars: list[Bar]) -> int:
        """保存板块行情"""
        
    def save_index_bars(self, bars: list[Bar]) -> int:
        """保存指数行情"""
        
    def get_sector_bars(
        self, 
        sector_id: str, 
        start: date, 
        end: date,
        freq: str = "day"  # 'day', 'week', 'month'
    ) -> pl.DataFrame:
        """获取板块行情，支持多周期"""
        
    def get_index_bars(
        self, 
        symbol: str, 
        start: date, 
        end: date,
        freq: str = "day"
    ) -> pl.DataFrame:
        """获取指数行情，支持多周期"""
        
    def get_stock_bars(
        self, 
        symbol: str, 
        start: date, 
        end: date,
        freq: str = "day"
    ) -> pl.DataFrame:
        """获取个股行情，支持多周期（从日线重采样）"""
```

## 5. 重采样工具

### 5.1 Resampler 类

```python
class Resampler:
    """K线数据重采样器"""
    
    @staticmethod
    def daily_to_weekly(df: pl.DataFrame) -> pl.DataFrame:
        """日线转周线"""
        return df.groupby_dynamic(
            index_column="dt",
            every="1w",
            period="1w",
            label="left"
        ).agg([
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
            pl.col("amount").sum().alias("amount"),
        ])
    
    @staticmethod
    def daily_to_monthly(df: pl.DataFrame) -> pl.DataFrame:
        """日线转月线"""
        return df.groupby_dynamic(
            index_column="dt",
            every="1mo",
            period="1mo",
            label="left"
        ).agg([
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
            pl.col("amount").sum().alias("amount"),
        ])
    
    @staticmethod
    def resample(df: pl.DataFrame, freq: str) -> pl.DataFrame:
        """通用重采样方法"""
        if freq == "day":
            return df
        elif freq == "week":
            return Resampler.daily_to_weekly(df)
        elif freq == "month":
            return Resampler.daily_to_monthly(df)
        else:
            raise ValueError(f"Unsupported frequency: {freq}")
```

## 6. 数据同步服务

### 6.1 SectorSyncService

```python
class SectorSyncService:
    """板块数据同步服务"""
    
    def __init__(self, dal: SectorDAL, fetcher: TushareFetcher):
        self.dal = dal
        self.fetcher = fetcher
    
    def sync_industry_sectors(self) -> int:
        """同步行业板块列表（从tushare）"""
        
    def sync_concept_sectors(self) -> int:
        """同步概念板块列表（从tushare）"""
        
    def sync_sector_stocks(self, sector_id: str) -> int:
        """同步板块成分股"""
        
    def sync_sector_bars(self, sector_id: str, start: date, end: date) -> int:
        """同步板块行情"""
```

### 6.2 IndexSyncService

```python
class IndexSyncService:
    """指数数据同步服务"""
    
    def __init__(self, dal: IndexDAL, bar_dal: BarDAL, fetcher: TushareFetcher):
        self.dal = dal
        self.bar_dal = bar_dal
        self.fetcher = fetcher
    
    def sync_index_list(self) -> int:
        """同步指数列表（从tushare）"""
        
    def sync_index_bars(self, symbol: str, start: date, end: date) -> int:
        """同步指数行情"""
        
    def sync_all_index_bars(self, start: date, end: date) -> dict[str, int]:
        """同步所有指数行情"""
```

## 7. 定时任务配置

### 7.1 APScheduler 任务

```python
# 每日收盘后同步数据
scheduler.add_job(
    sync_sectors_daily,
    'cron',
    hour=19,  # 晚上7点
    minute=0,
    id='sync_sectors'
)

scheduler.add_job(
    sync_indices_daily,
    'cron',
    hour=19,
    minute=30,
    id='sync_indices'
)
```

## 8. 依赖关系

```
数据层依赖关系：

sectors (表)
  ↓
sector_stocks (表) - 依赖 sectors
  ↓
sector_bars (表) - 依赖 sectors

indices (表)
  ↓
index_bars (表) - 依赖 indices

实施顺序：
1. 创建 sectors 和 indices 表
2. 创建 sector_stocks 表
3. 创建 sector_bars 和 index_bars 表
4. 实现 Resampler 工具
5. 实现 DAL 层
6. 实现同步服务
7. 配置定时任务
```

## 9. 注意事项

1. **数据一致性**：板块和指数数据从tushare同步，需要处理重复同步的情况（upsert）
2. **性能考虑**：板块成分股可能较多，查询时需要优化
3. **重采样精度**：周线和月线按自然周/月聚合，注意节假日处理
4. **错误处理**：同步失败时记录日志，不影响其他数据同步

