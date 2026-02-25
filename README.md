# 交易所资金费率监控系统

前后端分离项目：Vue 3 前端 + Spring Boot 后端，从多个交易所 WebSocket 获取 BTC/ETH 永续期货资金费率、期货价格、现货价格，存储到 Redis，前端每秒轮询并按资金费率排序展示。

## 技术栈

- **前端**: Vue 3 + Vite + Axios
- **后端**: Spring Boot 3.2 + Java 17
- **存储**: Redis
- **数据源**: Binance、OKX、Bybit、Gate.io、MEXC、Bitget、CoinEx、Crypto.com（WebSocket）+ Kucoin、HTX、BingX、CoinW（HTTP 轮询）
- **连接管理**: 统一 WebSocket 客户端，支持断线自动重连（指数退避）

## 快速启动

### 1. 启动 Redis

需先安装并启动 Redis（默认 localhost:6379）。使用 Docker 时：

```bash
docker compose up -d redis
# 或
docker-compose up -d redis
```

### 2. 启动后端

```bash
cd backend
mvn spring-boot:run
```

### 3. 启动前端

```bash
cd frontend
npm install
npm run dev
```

访问 http://localhost:5173

## 功能说明

- **币种切换**: 下拉选择 BTC 或 ETH，仅展示该币种数据
- **数据列**: 交易所、资金费率、下次结算时间、期货价格、现货价格、价差
- **排序**: 按资金费率从大到小排序
- **刷新**: 前端每秒调用后端接口获取最新数据
- **断线重连**: 后端 WebSocket 断线后自动重连（1s → 2s → 4s → … 最大 60s）

## 开发文档

- [交易所 API 接入进度](docs/EXCHANGE_API_PROGRESS.md) - 各交易所 WebSocket/HTTP 调研结果及待完成任务

## 项目结构

```
experiment/
├── backend/              # Spring Boot 后端
│   └── src/main/java/com/experiment/
│       ├── config/       # Redis、CORS 配置
│       ├── controller/   # REST API
│       ├── model/        # 数据模型
│       ├── service/      # 业务逻辑、Redis 存储
│       └── websocket/    # 统一 WebSocket 客户端及交易所 Handler
├── frontend/             # Vue 3 前端
│   └── src/
│       ├── api/          # 接口调用
│       └── views/        # 主页面
└── docker-compose.yml
```
