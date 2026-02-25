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

public class BinanceHandler implements ExchangeWebSocketHandler {

    private static final String FUTURES_WS = "wss://fstream.binance.com/stream?streams=btcusdt@markPrice@1s/ethusdt@markPrice@1s/solusdt@markPrice@1s/xrpusdt@markPrice@1s/hypeusdt@markPrice@1s/dogeusdt@markPrice@1s/bnbusdt@markPrice@1s";
    private static final String SPOT_WS = "wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/ethusdt@ticker/solusdt@ticker/xrpusdt@ticker/hypeusdt@ticker/dogeusdt@ticker/bnbusdt@ticker";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BinanceHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createFuturesClient() {
        return new ManagedWebSocket("binance", URI.create(FUTURES_WS), this);
    }

    public ManagedWebSocket createSpotClient() {
        return new ManagedWebSocket("binance-spot", URI.create(SPOT_WS), new BinanceSpotHandler());
    }

    private void saveFundingRate(String symbol, BigDecimal rate, Long nextFundingTime) {
        if (rate != null) marketDataService.saveFundingRate("binance", symbol, rate, nextFundingTime);
    }

    private void saveFuturesPrice(String symbol, BigDecimal price) {
        if (price != null) marketDataService.saveFuturesPrice("binance", symbol, price);
    }

    private void saveSpotPrice(String symbol, BigDecimal price) {
        if (price != null) marketDataService.saveSpotPrice("binance", symbol, price);
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
        log.info("Binance futures WebSocket connected");
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            JsonNode stream = root.get("stream");
            if (stream == null) return;
            String streamName = stream.asText();
            JsonNode data = root.get("data");
            if (data == null) return;
            String symbol = data.get("s").asText();
            if (streamName.contains("markPrice")) {
                BigDecimal rate = parseDecimal(data, "r");
                Long nextFundingTime = data.has("T") ? data.get("T").asLong() : null;
                BigDecimal markPrice = parseDecimal(data, "p");
                saveFundingRate(symbol, rate, nextFundingTime);
                saveFuturesPrice(symbol, markPrice);
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("Binance parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("Binance parse error: {}", e.getMessage());
            }
        }
    }

    private class BinanceSpotHandler implements ExchangeWebSocketHandler {
        @Override
        public void onConnected(ManagedWebSocket client) {
            log.info("Binance spot WebSocket connected");
        }

        @Override
        public void onMessage(String message) {
            try {
                JsonNode root = objectMapper.readTree(message);
                JsonNode data = root.get("data");
                if (data == null) return;
                String symbol = data.has("s") ? data.get("s").asText() : null;
                BigDecimal price = parseDecimal(data, "c");
                if (symbol != null && price != null) saveSpotPrice(symbol, price);
            } catch (Exception e) {
                if (RedisShutdownUtil.isRedisShutdownException(e)) {
                    log.debug("Binance spot parse error (Redis shutdown): {}", e.getMessage());
                } else {
                    log.warn("Binance spot parse error: {}", e.getMessage());
                }
            }
        }
    }
}
