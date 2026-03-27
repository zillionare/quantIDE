# QMT Gateway

迅投QMT独立网关服务，提供实时行情推送和实盘交易功能。

## 功能特性

- **实时行情**: 订阅QMT全推行情，合成1分钟、30分钟和日线K线
  - 自动订阅上证、深圳、北京全市场个股行情
  - 单独订阅主要指数：上证指数、深成指、沪深300、中证500、中证1000、科创50
  - 通过WebSocket实时推送到客户端
- **交易执行**: 支持买入、卖出、撤单等操作
- **Web界面**: 提供实盘交易界面
- **WebSocket**: 实时推送行情数据

## 安装

```bash
pip install qmt-gateway
```

## 使用

### ASGI 启动

```bash
cd qmt-gateway
poetry install
poetry run uvicorn qmt_gateway.app:app --host 0.0.0.0 --port 8130
```

### 首次启动

首次启动时会自动进入初始化向导，配置：
1. 管理员账号
2. 服务器设置（端口、日志等）
3. QMT账号和路径

初始化过程大约需要1分钟。

### 强制重新初始化

访问以下URL强制重新初始化：
```
http://localhost:8130/init-wizard?force=true
```

## 配置

所有配置通过初始化向导设置，存储在SQLite数据库中。默认配置：

- 服务器端口: 8130
- 日志路径: ~/.qmt-gateway/log
- 日志轮转: 10MB
- 日志保留: 10个文件

### QMT路径配置

- **QMT路径**: 包含userdata_mini的父目录路径，例如 `C:\国金证券QMT交易端\userdata_mini`
- **xtquant路径**: 下载xtquant后的解压目录，例如 `C:\apps\xtquant`

## 开发

开发期同样通过 ASGI 入口启动，不再保留单独的 CLI 发布入口：

```bash
cd qmt-gateway
poetry install
poetry run uvicorn qmt_gateway.app:app --host 0.0.0.0 --port 8130
```

## 技术栈

- FastHTML + MonsterUI
- SQLite + sqlite-utils
- loguru
- APScheduler
- WebSocket

## License

MIT
