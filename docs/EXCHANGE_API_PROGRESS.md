# 交易所 API 接入进度文档

本文档记录各交易所 WebSocket/HTTP API 调研结果及实现进度，便于任务重启后继续完成。

---

## 一、已完成实现（WebSocket）

以下 8 个交易所已通过 WebSocket 接入，使用统一 `ManagedWebSocket` 客户端，支持断线重连：

| 交易所 | 连接数 | WebSocket 地址 | 数据 |
|--------|--------|----------------|------|
| Binance | 2 | 期货: `wss://fstream.binance.com/stream`<br>现货: `wss://stream.binance.com:9443/stream` | 资金费率、期货价、现货价 |
| OKX | 1 | `wss://ws.okx.com:8443/ws/v5/public` | funding-rate、tickers |
| Bybit | 2 | 期货: `wss://stream.bybit.com/v5/public/linear`<br>现货: `wss://stream.bybit.com/v5/public/spot` | tickers |
| Gate.io | 1 | `wss://fx-ws.gateio.ws/v4/ws/usdt` | futures.tickers |
| MEXC | 1 | `wss://contract.mexc.com/edge` | sub.ticker |
| Bitget | 1 | `wss://ws.bitget.com/v2/ws/public` | ticker (mc) |
| Coinbase | 1 | `wss://advanced-trade-ws.coinbase.com` | ticker 现货 (BTC-USD, ETH-USD) |
| CoinEx | 1 | `wss://socket.coinex.com/v2/futures` | state.subscribe 资金费率、期货价、现货价 |
| Crypto.com | 1 | `wss://stream.crypto.com/exchange/v1/market` | funding、ticker、mark、index (BTCUSD-PERP, ETHUSD-PERP) |

---

## 二、待接入交易所调研结果

### 2.1 Kucoin

**WebSocket：**
- 期货: `wss://x-push-futures.kucoin.com` 或 `wss://ws-api-futures.kucoin.com`
- 有 ticker 频道
- 资金费率：`/contract/announcement:{symbol}` 每 8 小时推送

**HTTP（已确认）：**
- 资金费率: `GET https://api.kucoin.com/api/ua/v1/market/funding-rate?symbol=XBTUSDTM`
  - 参数: `symbol` = XBTUSDTM (BTC), ETHUSDTM (ETH)
  - 响应: `nextFundingRate`, `fundingTime`
- Ticker: `GET https://api-futures.kucoin.com/api/v1/ticker?symbol=XBTUSDTM`
  - 需确认 symbol 格式（XBTUSDTM 或 BTCUSDTM）

### 2.2 HTX (Huobi)

**WebSocket：**
- `public.$contract_code.funding_rate` 可订阅资金费率
- 无需认证

**HTTP（已确认）：**
- 资金费率: `GET https://api.hbdm.com/linear-swap-api/v1/swap_funding_rate?contract_code=BTC-USDT`
  - 参数: `contract_code` = BTC-USDT, ETH-USDT
- 需补充: ticker/价格接口（swap_market_detail 等）

### 2.3 BingX

**WebSocket：**
- 文档: `https://bingx-api.github.io/docs/#/en-us/swapV2/socket/market`
- 需查具体订阅格式

**HTTP（已确认）：**
- 资金费率: `GET https://open-api.bingx.com/openApi/swap/v2/quote/fundingRate?symbol=BTC-USDT`
- Ticker: 需查 `swapV2PublicGetQuoteTicker` 或类似接口

### 2.4 CoinW

**WebSocket：**
- 文档提及有 WebSocket 实时资金费率，但具体格式未查

**HTTP（已确认）：**
- 资金费率: `GET https://api.coinw.com/v1/perpum/fundingRate?instrument=btc`
  - 参数: `instrument` = btc, eth（小写）
  - 响应: `data.value` 为资金费率
  - 注意: 返回的是**上次结算**费率，非实时
- 需补充: ticker/价格接口

### 2.5 Coinbase

**WebSocket：**
- Advanced Trade: `wss://advanced-trade-ws.coinbase.com`
- 有 ticker、ticker_batch 频道（现货 BTC-USD, ETH-USD）
- 永续期货产品: BTC-PERP-INTX 等
- 资金费率: 未在公开 WebSocket 频道中找到

**HTTP：**
- 历史资金费率: Derivatives API `GET /rest/funding-rate`（需查完整 URL）

### 2.6 Crypto.com

**WebSocket：**
- `funding.{instrument_name}` - 固定小时费率
- `estimatedfunding.{instrument_name}` - 预估费率

### 2.7 CoinEx

**WebSocket：**
- `state.subscribe` 订阅市场状态
- 参数: `{"method":"state.subscribe","params":{"market_list":["BTCUSDT"]}}`
- 返回: `latest_funding_rate`, `next_funding_rate`, `latest_funding_time`, `next_funding_time`

### 2.8 Kraken

**WebSocket：**
- 期货: `wss://futures.kraken.com/ws/v1`
- 需每 60 秒 ping
- 资金费率可能在私有频道

**HTTP：**
- 历史资金费率: `GET /historical-funding-rates`

### 2.9 Hyperliquid

**WebSocket：**
- `wss://api.hyperliquid.xyz/ws`
- `userFundings` 为用户资金，非公开

**HTTP：**
- `fundingHistory` info 端点获取历史资金费率

### 2.10 dYdX v4

**WebSocket：**
- 有 trades、markets 等频道

**HTTP：**
- Indexer `getPerpetualMarketHistoricalFunding()` 获取历史资金费率

---

## 三、待完成任务清单

### 3.1 HTTP 轮询基础设施（已完成）

- [x] 创建 `HttpExchangeFetcher` 接口
- [x] 创建 `HttpPollingRunner`，每秒调用各交易所 HTTP 接口
- [x] 将结果写入 Redis（复用 `MarketDataService`）

### 3.2 实现 HTTP Fetcher（已完成）

- [x] **Kucoin**: 调用 funding-rate + ticker，解析后写入 Redis
- [x] **HTX**: 调用 swap_funding_rate + detail/merged + swap_index
- [x] **BingX**: 调用 fundingRate + price
- [x] **CoinW**: 调用 fundingRate + perpumPublic/depth
- [x] **Coinbase**: 尝试 INTX funding（需认证，未配置则静默跳过）

### 3.3 补充调研（已完成）

- [x] HTX: swap_market_detail + swap_index 已实现
- [x] BingX: swap ticker 价格接口已实现
- [x] Kucoin: 期货 ticker symbol 格式（XBTUSDTM）已确认

### 3.4 可选：WebSocket 扩展（部分完成）

- [ ] Kucoin WebSocket ticker（当前用 HTTP）
- [ ] HTX WebSocket funding_rate（当前用 HTTP）
- [x] Crypto.com WebSocket funding、ticker、mark、index
- [x] CoinEx WebSocket state.subscribe

### 3.5 更新前端与后端（已完成）

- [x] 在 `MarketDataService.getMarketDataBySymbol` 中加入新交易所
- [x] 在前端 `exchangeLabel` 中加入新交易所显示名称

---

## 四、项目结构参考

```
backend/src/main/java/com/experiment/
├── rest/
│   ├── HttpExchangeFetcher.java
│   ├── HttpPollingRunner.java
│   └── fetcher/
│       ├── KucoinFetcher.java
│       ├── HtxFetcher.java
│       ├── BingxFetcher.java
│       ├── CoinwFetcher.java
│       └── CoinbaseFetcher.java
├── websocket/
│   ├── ManagedWebSocket.java
│   ├── ExchangeWebSocketHandler.java
│   └── handler/
│       ├── BinanceHandler.java
│       ├── OkxHandler.java
│       └── ...
└── service/
    └── MarketDataService.java
```

---

## 五、Redis Key 约定

- 资金费率: `funding:{exchange}:{symbol}` 例: `funding:kucoin:BTCUSDT`
- 期货价格: `futures:{exchange}:{symbol}`
- 现货价格: `spot:{exchange}:{symbol}`

---

*文档更新时间: 2025-02*
