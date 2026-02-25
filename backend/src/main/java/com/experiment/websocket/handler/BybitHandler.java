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

public class BybitHandler implements ExchangeWebSocketHandler {

    private static final String FUTURES_WS = "wss://stream.bybit.com/v5/public/linear";
    private static final String SPOT_WS = "wss://stream.bybit.com/v5/public/spot";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BybitHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createFuturesClient() {
        return new ManagedWebSocket("bybit", URI.create(FUTURES_WS), this);
    }

    public ManagedWebSocket createSpotClient() {
        return new ManagedWebSocket("bybit-spot", URI.create(SPOT_WS), new BybitSpotHandler());
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

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("Bybit futures WebSocket connected");
        client.send("{\"op\":\"subscribe\",\"args\":[\"tickers.BTCUSDT\",\"tickers.ETHUSDT\",\"tickers.SOLUSDT\",\"tickers.XRPUSDT\",\"tickers.HYPEUSDT\",\"tickers.DOGEUSDT\",\"tickers.BNBUSDT\"]}");
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            if (root.has("ret_msg") && !"OK".equals(root.path("ret_msg").asText())) return;
            JsonNode topic = root.get("topic");
            if (topic == null) return;
            JsonNode data = root.get("data");
            if (data == null) return;
            String symbol = data.has("symbol") ? data.get("symbol").asText() : null;
            if (symbol == null) return;

            BigDecimal fundingRate = parseDecimal(data, "fundingRate");
            Long nextFundingTime = data.has("nextFundingTime") && !data.get("nextFundingTime").asText().isEmpty()
                    ? Long.parseLong(data.get("nextFundingTime").asText()) : null;
            BigDecimal lastPrice = parseDecimal(data, "lastPrice");
            BigDecimal markPrice = parseDecimal(data, "markPrice");

            // 仅在有值时保存，避免 delta 更新中缺失字段时用 null 覆盖已有数据
            if (fundingRate != null) {
                marketDataService.saveFundingRate("bybit", symbol, fundingRate, nextFundingTime);
            }
            BigDecimal futuresPrice = markPrice != null ? markPrice : lastPrice;
            if (futuresPrice != null) {
                marketDataService.saveFuturesPrice("bybit", symbol, futuresPrice);
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("Bybit parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("Bybit parse error: {}", e.getMessage());
            }
        }
    }

    private class BybitSpotHandler implements ExchangeWebSocketHandler {
        @Override
        public void onConnected(ManagedWebSocket client) {
            log.info("Bybit spot WebSocket connected");
            client.send("{\"op\":\"subscribe\",\"args\":[\"tickers.BTCUSDT\",\"tickers.ETHUSDT\",\"tickers.SOLUSDT\",\"tickers.XRPUSDT\",\"tickers.HYPEUSDT\",\"tickers.DOGEUSDT\",\"tickers.BNBUSDT\"]}");
        }

        @Override
        public void onMessage(String message) {
            try {
                JsonNode root = objectMapper.readTree(message);
                JsonNode data = root.get("data");
                if (data == null) return;
                String symbol = data.has("symbol") ? data.get("symbol").asText() : null;
                BigDecimal lastPrice = parseDecimal(data, "lastPrice");
                if (symbol != null && lastPrice != null) {
                    marketDataService.saveSpotPrice("bybit", symbol, lastPrice);
                }
            } catch (Exception e) {
                if (RedisShutdownUtil.isRedisShutdownException(e)) {
                    log.debug("Bybit spot parse error (Redis shutdown): {}", e.getMessage());
                } else {
                    log.warn("Bybit spot parse error: {}", e.getMessage());
                }
            }
        }
    }
}
