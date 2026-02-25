package com.experiment.websocket.handler;

import com.experiment.service.MarketDataService;
import com.experiment.util.RedisShutdownUtil;
import com.experiment.websocket.ExchangeWebSocketHandler;
import com.experiment.websocket.ManagedWebSocket;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.net.URI;

/**
 * Coinbase Advanced Trade WebSocket - 现货 ticker (BTC-USD, ETH-USD)。
 * 期货/资金费率需 INTX API（需认证），暂不接入。
 */
public class CoinbaseHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://advanced-trade-ws.coinbase.com";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CoinbaseHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("coinbase", URI.create(WS_URL), this);
    }

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("Coinbase WebSocket connected");
        // 订阅 ticker + heartbeats 保持连接（60-90 秒无更新会断开）
        client.send("{\"type\":\"subscribe\",\"product_ids\":[\"BTC-USD\",\"ETH-USD\"],\"channel\":\"ticker\"}");
        client.send("{\"type\":\"subscribe\",\"channel\":\"heartbeats\"}");
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            String channel = root.has("channel") ? root.get("channel").asText() : "";
            if ("heartbeats".equals(channel)) return;

            if ("ticker".equals(channel) && root.has("events")) {
                for (JsonNode ev : root.get("events")) {
                    if (ev.has("tickers")) {
                        for (JsonNode t : ev.get("tickers")) {
                            String productId = t.has("product_id") ? t.get("product_id").asText() : "";
                            BigDecimal price = parseDecimal(t, "price");
                            if (price == null) continue;
                            String symbol = productId.contains("BTC") ? "BTCUSDT" : productId.contains("ETH") ? "ETHUSDT" : null;
                            if (symbol != null) {
                                marketDataService.saveSpotPrice("coinbase", symbol, price);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("Coinbase parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("Coinbase parse error: {}", e.getMessage());
            }
        }
    }

    private BigDecimal parseDecimal(JsonNode node, String key) {
        if (!node.has(key)) return null;
        String s = node.get(key).asText();
        if (s == null || s.isEmpty()) return null;
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
