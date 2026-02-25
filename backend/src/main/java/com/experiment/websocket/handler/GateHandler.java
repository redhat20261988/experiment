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

public class GateHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public GateHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("gateio", URI.create(WS_URL), this);
    }

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("Gate.io WebSocket connected");
        long time = System.currentTimeMillis() / 1000;
        client.send(String.format("{\"time\":%d,\"channel\":\"futures.tickers\",\"event\":\"subscribe\",\"payload\":[\"BTC_USDT\",\"ETH_USDT\",\"SOL_USDT\",\"XRP_USDT\",\"HYPE_USDT\",\"DOGE_USDT\",\"BNB_USDT\"]}", time));
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            if (!"futures.tickers".equals(root.has("channel") ? root.get("channel").asText() : "")) return;
            JsonNode result = root.get("result");
            if (result == null || !result.isArray()) return;

            for (JsonNode item : result) {
                String contract = item.has("contract") ? item.get("contract").asText() : "";
                String symbol = contract.replace("_", "");
                BigDecimal fundingRate = parseDecimal(item, "funding_rate");
                BigDecimal last = parseDecimal(item, "last");
                BigDecimal markPrice = parseDecimal(item, "mark_price");
                BigDecimal indexPrice = parseDecimal(item, "index_price");

                marketDataService.saveFundingRate("gateio", symbol, fundingRate, null);
                marketDataService.saveFuturesPrice("gateio", symbol, markPrice != null ? markPrice : last);
                // Gate.io期货ticker的indexPrice是现货价格，如果为null则不保存现货价格
                // 不应fallback到期货价格（last），因为会导致价差为0
                if (indexPrice != null) {
                    marketDataService.saveSpotPrice("gateio", symbol, indexPrice);
                }
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("Gate.io parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("Gate.io parse error: {}", e.getMessage());
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
