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

public class MexcHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://contract.mexc.com/edge";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MexcHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("mexc", URI.create(WS_URL), this);
    }

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("MEXC WebSocket connected");
        client.send("{\"method\":\"sub.ticker\",\"param\":{\"symbol\":\"BTC_USDT\"}}");
        client.send("{\"method\":\"sub.ticker\",\"param\":{\"symbol\":\"ETH_USDT\"}}");
        client.send("{\"method\":\"sub.ticker\",\"param\":{\"symbol\":\"SOL_USDT\"}}");
        client.send("{\"method\":\"sub.ticker\",\"param\":{\"symbol\":\"XRP_USDT\"}}");
        client.send("{\"method\":\"sub.ticker\",\"param\":{\"symbol\":\"HYPE_USDT\"}}");
        client.send("{\"method\":\"sub.ticker\",\"param\":{\"symbol\":\"DOGE_USDT\"}}");
        client.send("{\"method\":\"sub.ticker\",\"param\":{\"symbol\":\"BNB_USDT\"}}");
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            String channel = root.has("channel") ? root.get("channel").asText() : "";
            if ("pong".equals(channel)) return;

            if ("push.ticker".equals(channel)) {
                JsonNode data = root.get("data");
                if (data == null) return;
                String symbol = root.has("symbol") ? root.get("symbol").asText() : data.path("symbol").asText();
                symbol = symbol.replace("_", "");

                BigDecimal fundingRate = parseDecimal(data, "fundingRate");
                BigDecimal lastPrice = parseDecimal(data, "lastPrice");
                BigDecimal fairPrice = parseDecimal(data, "fairPrice");
                BigDecimal indexPrice = parseDecimal(data, "indexPrice");

                marketDataService.saveFundingRate("mexc", symbol, fundingRate, null);
                marketDataService.saveFuturesPrice("mexc", symbol, fairPrice != null ? fairPrice : lastPrice);
                // MEXC期货ticker的indexPrice是现货价格，如果为null则不保存现货价格
                // 不应fallback到期货价格（lastPrice），因为会导致价差为0
                if (indexPrice != null) {
                    marketDataService.saveSpotPrice("mexc", symbol, indexPrice);
                }
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("MEXC parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("MEXC parse error: {}", e.getMessage());
            }
        }
    }

    @Override
    public String getHeartbeatMessage() {
        return "{\"method\":\"ping\"}";
    }

    @Override
    public long getHeartbeatIntervalMs() {
        return 15_000;
    }

    private BigDecimal parseDecimal(JsonNode node, String key) {
        if (!node.has(key)) return null;
        JsonNode n = node.get(key);
        if (n.isNull()) return null;
        String s = n.asText();
        if (s == null || s.isEmpty()) return null;
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
