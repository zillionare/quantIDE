# 分析导航功能 - 后端API设计文档

## 1. 概述

本文档描述分析导航功能的后端API设计，包括板块管理、指数管理和K线数据查询接口。

## 2. API路由结构

```
/api/v1/
├── sectors/                    # 板块管理
│   ├── GET    /              # 列出板块
│   ├── POST   /              # 创建板块
│   ├── GET    /{id}          # 获取板块详情
│   ├── PUT    /{id}          # 更新板块
│   ├── DELETE /{id}          # 删除板块
│   ├── GET    /{id}/stocks   # 获取板块成分股
│   ├── POST   /{id}/stocks   # 添加成分股
│   ├── DELETE /{id}/stocks/{symbol}  # 删除成分股
│   └── POST   /{id}/import   # 从文件导入成分股
│
├── indices/                    # 指数管理
│   ├── GET    /              # 列出指数
│   └── GET    /{symbol}      # 获取指数详情
│
└── kline/                      # K线数据
    ├── GET    /stock/{symbol}        # 个股K线
    ├── GET    /sector/{sector_id}    # 板块K线
    └── GET    /index/{symbol}        # 指数K线
```

## 3. 板块管理API

### 3.1 列出板块

**请求**
```
GET /api/v1/sectors?type=custom&page=1&size=20
```

**参数**
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| type | string | 否 | 板块类型：custom/industry/concept |
| page | int | 否 | 页码，默认1 |
| size | int | 否 | 每页数量，默认20 |

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "items": [
      {
        "id": "custom_001",
        "name": "我的自选股",
        "sector_type": "custom",
        "source": "user",
        "description": "",
        "stock_count": 10,
        "created_at": "2024-01-15T10:00:00Z",
        "updated_at": "2024-01-15T10:00:00Z"
      }
    ],
    "total": 1,
    "page": 1,
    "size": 20
  }
}
```

### 3.2 创建板块

**请求**
```
POST /api/v1/sectors
Content-Type: application/json

{
  "name": "新能源板块",
  "sector_type": "custom",
  "description": "新能源相关股票"
}
```

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "custom_001",
    "name": "新能源板块",
    "sector_type": "custom",
    "source": "user",
    "description": "新能源相关股票",
    "created_at": "2024-01-15T10:00:00Z",
    "updated_at": "2024-01-15T10:00:00Z"
  }
}
```

### 3.3 获取板块详情

**请求**
```
GET /api/v1/sectors/custom_001
```

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "custom_001",
    "name": "新能源板块",
    "sector_type": "custom",
    "source": "user",
    "description": "新能源相关股票",
    "stock_count": 10,
    "created_at": "2024-01-15T10:00:00Z",
    "updated_at": "2024-01-15T10:00:00Z"
  }
}
```

### 3.4 更新板块

**请求**
```
PUT /api/v1/sectors/custom_001
Content-Type: application/json

{
  "name": "新能源板块（更新）",
  "description": "更新后的描述"
}
```

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "custom_001",
    "name": "新能源板块（更新）",
    "sector_type": "custom",
    "source": "user",
    "description": "更新后的描述",
    "updated_at": "2024-01-15T11:00:00Z"
  }
}
```

### 3.5 删除板块

**请求**
```
DELETE /api/v1/sectors/custom_001
```

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": null
}
```

### 3.6 获取板块成分股

**请求**
```
GET /api/v1/sectors/custom_001/stocks
```

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": [
    {
      "sector_id": "custom_001",
      "symbol": "000001.SZ",
      "name": "平安银行",
      "weight": 0.0,
      "added_at": "2024-01-15T10:00:00Z"
    },
    {
      "sector_id": "custom_001",
      "symbol": "600519.SH",
      "name": "贵州茅台",
      "weight": 0.0,
      "added_at": "2024-01-15T10:00:00Z"
    }
  ]
}
```

### 3.7 添加成分股

**请求**
```
POST /api/v1/sectors/custom_001/stocks
Content-Type: application/json

{
  "symbol": "000001.SZ",
  "name": "平安银行"
}
```

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "sector_id": "custom_001",
    "symbol": "000001.SZ",
    "name": "平安银行",
    "weight": 0.0,
    "added_at": "2024-01-15T10:00:00Z"
  }
}
```

### 3.8 删除成分股

**请求**
```
DELETE /api/v1/sectors/custom_001/stocks/000001.SZ
```

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": null
}
```

### 3.9 从文件导入成分股

**请求**
```
POST /api/v1/sectors/custom_001/import
Content-Type: multipart/form-data

file: [文件内容]
```

**文件格式**
- 每行一个股票代码
- 支持格式：`000001.SZ` 或 `000001`
- 可选：代码后加空格和名称，如 `000001.SZ 平安银行`

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "total": 10,
    "success": 8,
    "failed": 2,
    "failed_symbols": ["invalid_code", "999999.XY"]
  }
}
```

## 4. 指数管理API

### 4.1 列出指数

**请求**
```
GET /api/v1/indices?type=market&category=上证系列&page=1&size=20
```

**参数**
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| type | string | 否 | 指数类型：market/industry/concept |
| category | string | 否 | 分类：上证系列/深证系列/中证系列等 |
| page | int | 否 | 页码，默认1 |
| size | int | 否 | 每页数量，默认20 |

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "items": [
      {
        "symbol": "000001.SH",
        "name": "上证指数",
        "index_type": "market",
        "category": "上证系列",
        "publisher": "上海证券交易所",
        "base_date": "1990-12-19",
        "base_point": 100.0,
        "list_date": "1991-07-15"
      }
    ],
    "total": 50,
    "page": 1,
    "size": 20
  }
}
```

### 4.2 获取指数详情

**请求**
```
GET /api/v1/indices/000001.SH
```

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "symbol": "000001.SH",
    "name": "上证指数",
    "index_type": "market",
    "category": "上证系列",
    "publisher": "上海证券交易所",
    "base_date": "1990-12-19",
    "base_point": 100.0,
    "list_date": "1991-07-15",
    "description": "反映上海证券交易所上市股票价格的整体表现"
  }
}
```

## 5. K线数据API

### 5.1 个股K线数据

**请求**
```
GET /api/v1/kline/stock/000001.SZ?start=2024-01-01&end=2024-01-31&freq=day&ma=5,10,20,60
```

**参数**
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| start | string | 是 | 开始日期，格式：YYYY-MM-DD |
| end | string | 是 | 结束日期，格式：YYYY-MM-DD |
| freq | string | 否 | 周期：day/week/month，默认day |
| ma | string | 否 | 均线周期，逗号分隔，如：5,10,20,60 |
| compare | string | 否 | 对比指数代码，如：000001.SH |

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "symbol": "000001.SZ",
    "name": "平安银行",
    "freq": "day",
    "bars": [
      {
        "dt": "2024-01-02",
        "open": 10.5,
        "high": 10.8,
        "low": 10.4,
        "close": 10.6,
        "volume": 1000000,
        "amount": 10600000.0,
        "ma5": 10.55,
        "ma10": 10.52,
        "ma20": 10.50,
        "ma60": 10.45
      }
    ],
    "compare": {
      "symbol": "000001.SH",
      "name": "上证指数",
      "bars": [
        {
          "dt": "2024-01-02",
          "close": 2950.0
        }
      ]
    }
  }
}
```

### 5.2 板块K线数据

**请求**
```
GET /api/v1/kline/sector/custom_001?start=2024-01-01&end=2024-01-31&freq=day&ma=5,10,20
```

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "sector_id": "custom_001",
    "name": "新能源板块",
    "freq": "day",
    "bars": [
      {
        "dt": "2024-01-02",
        "open": 1000.0,
        "high": 1020.0,
        "low": 990.0,
        "close": 1010.0,
        "volume": 50000000,
        "amount": 500000000.0,
        "ma5": 1005.0,
        "ma10": 1002.0,
        "ma20": 998.0
      }
    ]
  }
}
```

### 5.3 指数K线数据

**请求**
```
GET /api/v1/kline/index/000001.SH?start=2024-01-01&end=2024-01-31&freq=day&ma=5,10,20,60
```

**响应**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "symbol": "000001.SH",
    "name": "上证指数",
    "freq": "day",
    "bars": [
      {
        "dt": "2024-01-02",
        "open": 2950.0,
        "high": 2980.0,
        "low": 2940.0,
        "close": 2970.0,
        "volume": 300000000,
        "amount": 3000000000.0,
        "ma5": 2960.0,
        "ma10": 2955.0,
        "ma20": 2950.0,
        "ma60": 2940.0
      }
    ]
  }
}
```

## 6. 错误处理

### 6.1 统一错误响应格式

```json
{
  "code": 10001,
  "message": "板块不存在",
  "data": null
}
```

### 6.2 错误码定义

| 错误码 | 说明 |
|--------|------|
| 0 | 成功 |
| 10001 | 板块不存在 |
| 10002 | 指数不存在 |
| 10003 | 股票不存在 |
| 10004 | 股票代码格式错误 |
| 10005 | 日期格式错误 |
| 10006 | 无效的周期类型 |
| 10007 | 文件格式错误 |
| 10008 | 板块名称已存在 |
| 50000 | 服务器内部错误 |

## 7. 性能优化

### 7.1 数据缓存

- K线数据缓存：使用 Redis 缓存热门股票/指数的K线数据
- 缓存过期时间：日线数据1小时，周月线数据1天

### 7.2 分页优化

- 板块列表、指数列表使用分页
- 成分股列表使用分页（每页50条）

### 7.3 批量操作

- 导入成分股支持批量插入
- K线数据查询支持批量symbols（逗号分隔）

## 8. 安全考虑

1. **权限控制**
   - 板块管理需要登录
   - 用户只能操作自己创建的板块（custom类型）
   - industry/concept类型只读

2. **输入验证**
   - 股票代码格式验证
   - 日期范围验证（最大365天）
   - 文件大小限制（最大1MB）

3. **限流**
   - K线数据接口：每分钟60次
   - 其他接口：每分钟120次
