# 交易所资金费率/期货价格为空 - 排查报告

## 一、问题概述

Bybit、Bitget、Coinbase、CoinEx、Crypto.com 五个交易所的资金费率、期货价格显示为空（"-"）。

## 二、排查结果与修复

### 2.1 Coinbase —— 设计如此，无需修复

**原因**：Coinbase 公开 API 不提供永续期货数据。

- **WebSocket**：仅订阅现货 ticker（BTC-USD, ETH-USD），只写入现货价格
- **HTTP**：INTX 资金费率接口需 API 认证，未配置时 401 静默跳过
- **代码注释**：`期货/资金费率需 INTX API（需认证），暂不接入`

**结论**：资金费率和期货价格会持续为空，只有现货价格。若需完整数据，需配置 Coinbase INTX API 认证。

---

### 2.2 Bitget —— 已修复

**原因**：订阅使用了错误的 `instType`。

- **错误**：`instType: "mc"` 对应现货/杠杆频道，不包含资金费率和期货价格
- **正确**：永续合约应使用 `instType: "USDT-FUTURES"`

**修复**：已将 BitgetHandler 中的 `instType` 从 `"mc"` 改为 `"USDT-FUTURES"`。

---

### 2.3 Bybit —— 已修复

**原因**：Bybit v5 ticker 使用 delta 更新，未变更字段不会出现在消息中。

- 当仅价格变化时，消息可能只含 `lastPrice`，不含 `fundingRate`
- 原逻辑会把 `fundingRate = null` 写入 Redis，覆盖已有有效值

**修复**：仅在解析到非空值时调用 `saveFundingRate` / `saveFuturesPrice`，避免用 null 覆盖已有数据。

---

### 2.4 CoinEx —— 待进一步验证

**当前实现**：`state.subscribe` 订阅 `["BTCUSDT","ETHUSDT"]`，解析 `state.update` 中的 `latest_funding_rate`、`mark_price`、`index_price`。

**可能原因**：

1. **市场名称**：CoinEx 期货市场名称为 `BTCUSDT`，与当前实现一致
2. **推送频率**：约 200ms 一次，若长时间无更新可能导致数据缺失
3. **过滤条件**：`market.endsWith("USDT") && !market.contains("_")` 可能过滤掉部分市场

**建议**：增加 debug 日志，确认是否收到 `state.update` 及 `state_list` 内容；若始终无数据，需对照 CoinEx 官方文档核对请求/响应格式。

---

### 2.5 Crypto.com —— 连接频繁断开

**现象**：日志中 Crypto.com WebSocket 频繁出现 `连接关闭: 1000`，随后重连。

**可能原因**：

1. **服务端主动关闭**：Crypto.com 可能对连接时长或心跳有要求
2. **订阅格式**：`funding.BTCUSD-PERP`、`ticker.BTCUSD-PERP` 等需与官方文档一致
3. **响应解析**：`params.result` / `params.data` 的实际结构可能与代码假设不符

**建议**：

1. 查阅 Crypto.com Exchange WebSocket 文档，确认心跳与连接保持要求
2. 打印原始消息，核对 funding/ticker 的实际 JSON 结构
3. 若为服务端超时，可缩短心跳间隔或增加 ping 保活

---

## 三、已修改文件

| 文件 | 修改内容 |
|------|----------|
| `BitgetHandler.java` | `instType` 从 `"mc"` 改为 `"USDT-FUTURES"` |
| `BybitHandler.java` | 仅在非空时调用 `saveFundingRate` / `saveFuturesPrice` |

## 四、验证步骤

1. 重启后端服务
2. 等待 WebSocket 连接建立（约 10–30 秒）
3. 刷新前端页面，检查 Bybit、Bitget 是否已有资金费率和期货价格
4. Coinbase 仍会为空（设计如此）
5. CoinEx、Crypto.com 若仍为空，需按上文建议进一步排查
