package com.experiment.websocket.handler;

import com.experiment.service.MarketDataService;
import com.experiment.util.RedisShutdownUtil;
import com.experiment.websocket.ExchangeWebSocketHandler;
import com.experiment.websocket.ManagedWebSocket;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * CoinEx 期货 WebSocket - state.subscribe 获取资金费率、mark_price、index_price。
 */
public class CoinExHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://socket.coinex.com/v2/futures";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile long lastStateUpdateTimeMs = 0;

    public CoinExHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("coinex", URI.create(WS_URL), this);
    }

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("CoinEx WebSocket connected");
        String subscribeMsg = "{\"method\":\"state.subscribe\",\"params\":{\"market_list\":[\"BTCUSDT\",\"ETHUSDT\",\"SOLUSDT\",\"XRPUSDT\",\"HYPEUSDT\",\"DOGEUSDT\",\"BNBUSDT\"]},\"id\":1}";
        log.info("CoinEx sending subscribe: {}", subscribeMsg);
        if (client != null && client.isOpen()) {
            client.send(subscribeMsg);
        } else {
            log.warn("CoinEx client not open, cannot send subscribe");
        }
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            
            String method = root.has("method") ? root.get("method").asText() : "";
            
            
            // 处理订阅确认消息 - 可能是result字段或method为state.subscribe的响应
            if (root.has("id") && root.has("result")) {
                log.info("CoinEx subscription confirmed: {}", message);
                return;
            }
            if (root.has("id") && "state.subscribe".equals(method)) {
                log.info("CoinEx subscription response: {}", message);
                return;
            }
            
            // 记录所有收到的消息以便调试（INFO级别以便排查问题）
            log.info("CoinEx received message - method: {}, message length: {}", method, message.length());
            log.info("CoinEx message content: {}", message.length() > 800 ? message.substring(0, 800) + "..." : message);
            
            // 只处理 state.update 消息
            if (!"state.update".equals(method)) {
                if (!method.isEmpty()) {
                    log.info("CoinEx ignoring message with method: {}", method);
                }
                return;
            }

            long now = System.currentTimeMillis();
            if (lastStateUpdateTimeMs > 0) {
                long intervalMs = now - lastStateUpdateTimeMs;
                if (intervalMs > 2000) {
                    log.info("CoinEx state.update 推送间隔 {}ms (超过2s，Redis TTL会过期)", intervalMs);
                } else {
                    log.debug("CoinEx state.update 推送间隔 {}ms", intervalMs);
                }
            }
            lastStateUpdateTimeMs = now;

            JsonNode data = root.get("data");
            if (data == null) {
                log.warn("CoinEx state.update message missing data field");
                return;
            }
            
            if (!data.has("state_list") || !data.get("state_list").isArray()) {
                log.warn("CoinEx state.update message missing or invalid state_list");
                return;
            }

            boolean hasData = false;
            for (JsonNode item : data.get("state_list")) {
                String market = item.has("market") ? item.get("market").asText() : "";
                if (market.isEmpty() || !market.endsWith("USDT") || market.contains("_")) {
                    continue; // 跳过无效的市场名称
                }
                
                String symbol = market;
                BigDecimal fundingRate = parseDecimal(item, "latest_funding_rate");
                Long nextFundingTime = item.has("next_funding_time") && !item.get("next_funding_time").isNull()
                        ? item.get("next_funding_time").asLong() : null;
                BigDecimal markPrice = parseDecimal(item, "mark_price");
                BigDecimal indexPrice = parseDecimal(item, "index_price");


                if (fundingRate != null) {
                    marketDataService.saveFundingRate("coinex", symbol, fundingRate, nextFundingTime);
                    hasData = true;
                    log.info("CoinEx saved funding rate for {}: {}", symbol, fundingRate);
                }
                if (markPrice != null) {
                    marketDataService.saveFuturesPrice("coinex", symbol, markPrice);
                    hasData = true;
                    log.info("CoinEx saved futures price for {}: {}", symbol, markPrice);
                }
                if (indexPrice != null) {
                    marketDataService.saveSpotPrice("coinex", symbol, indexPrice);
                    hasData = true;
                    log.info("CoinEx saved spot price for {}: {}", symbol, indexPrice);
                }
            }
            
            if (!hasData) {
                log.warn("CoinEx state.update message processed but no valid data found for BTCUSDT/ETHUSDT");
                log.info("CoinEx state_list items: {}", data.get("state_list").size());
                // 记录第一个item的market名称以便调试
                if (data.get("state_list").size() > 0) {
                    JsonNode firstItem = data.get("state_list").get(0);
                    String firstMarket = firstItem.has("market") ? firstItem.get("market").asText() : "unknown";
                    log.info("CoinEx first market in list: {}", firstMarket);
                }
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("CoinEx parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("CoinEx parse error: {} - Message: {}", e.getMessage(), message.length() > 200 ? message.substring(0, 200) + "..." : message);
            }
        }
    }

    // CoinEx API 文档中没有提到心跳机制，可能不需要或使用不同的方式
    // 如果连接频繁断开，可能需要调整心跳间隔或移除心跳
    @Override
    public String getHeartbeatMessage() {
        // 根据CoinEx API文档，可能不需要心跳，先返回null
        // 如果连接仍然不稳定，可以尝试使用ping消息
        return null;
        // return "{\"method\":\"server.ping\",\"params\":[],\"id\":0}";
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
