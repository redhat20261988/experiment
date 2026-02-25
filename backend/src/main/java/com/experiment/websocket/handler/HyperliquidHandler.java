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
 * Hyperliquid WebSocket - 永续期货资金费率、期货价格、现货价格。
 * WebSocket: wss://api.hyperliquid.xyz/ws
 */
public class HyperliquidHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://api.hyperliquid.xyz/ws";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public HyperliquidHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("hyperliquid", URI.create(WS_URL), this);
    }

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("Hyperliquid WebSocket connected");
        // Subscribe to price data (allMids) - funding rates need HTTP API
        try {
            String subscribe = "{\"method\":\"subscribe\",\"subscription\":{\"type\":\"allMids\"}}";
            client.send(subscribe);
        } catch (Exception e) {
            log.warn("Hyperliquid subscribe error: {}", e.getMessage());
        }
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            
            // Hyperliquid allMids format: {"channel":"allMids","data":{"mids":{...}}}
            // mids is a map of coin symbols to mid prices
            if (root.has("data") && root.has("channel")) {
                String channel = root.get("channel").asText();
                JsonNode data = root.get("data");
                
                if ("allMids".equals(channel) && data.has("mids")) {
                    JsonNode mids = data.get("mids");
                    // Extract BTC and ETH prices from mids map
                    // Hyperliquid allMids提供的是期货中间价，只保存期货价格
                    // 现货价格需要从现货API单独获取
                    if (mids.has("BTC")) {
                        BigDecimal btcPrice = parseDecimal(mids, "BTC");
                        if (btcPrice != null) {
                            marketDataService.saveFuturesPrice("hyperliquid", "BTCUSDT", btcPrice);
                        }
                    }
                    if (mids.has("ETH")) {
                        BigDecimal ethPrice = parseDecimal(mids, "ETH");
                        if (ethPrice != null) {
                            marketDataService.saveFuturesPrice("hyperliquid", "ETHUSDT", ethPrice);
                        }
                    }
                    if (mids.has("SOL")) {
                        BigDecimal solPrice = parseDecimal(mids, "SOL");
                        if (solPrice != null) {
                            marketDataService.saveFuturesPrice("hyperliquid", "SOLUSDT", solPrice);
                        }
                    }
                    if (mids.has("XRP")) {
                        BigDecimal xrpPrice = parseDecimal(mids, "XRP");
                        if (xrpPrice != null) {
                            marketDataService.saveFuturesPrice("hyperliquid", "XRPUSDT", xrpPrice);
                        }
                    }
                    if (mids.has("HYPE")) {
                        BigDecimal hypePrice = parseDecimal(mids, "HYPE");
                        if (hypePrice != null) {
                            marketDataService.saveFuturesPrice("hyperliquid", "HYPEUSDT", hypePrice);
                        }
                    }
                    if (mids.has("DOGE")) {
                        BigDecimal dogePrice = parseDecimal(mids, "DOGE");
                        if (dogePrice != null) {
                            marketDataService.saveFuturesPrice("hyperliquid", "DOGEUSDT", dogePrice);
                        }
                    }
                    if (mids.has("BNB")) {
                        BigDecimal bnbPrice = parseDecimal(mids, "BNB");
                        if (bnbPrice != null) {
                            marketDataService.saveFuturesPrice("hyperliquid", "BNBUSDT", bnbPrice);
                        }
                    }
                }
            } else {
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("Hyperliquid parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("Hyperliquid parse error: {}", e.getMessage());
            }
        }
    }

    private String extractSymbol(JsonNode root) {
        // Hyperliquid uses coin names like "BTC", "ETH"
        if (root.has("coin")) {
            String coin = root.get("coin").asText();
            if ("BTC".equals(coin)) return "BTCUSDT";
            if ("ETH".equals(coin)) return "ETHUSDT";
            if ("SOL".equals(coin)) return "SOLUSDT";
            if ("XRP".equals(coin)) return "XRPUSDT";
            if ("HYPE".equals(coin)) return "HYPEUSDT";
            if ("DOGE".equals(coin)) return "DOGEUSDT";
            if ("BNB".equals(coin)) return "BNBUSDT";
        }
        if (root.has("symbol")) {
            String sym = root.get("symbol").asText();
            if (sym.contains("BTC")) return "BTCUSDT";
            if (sym.contains("ETH")) return "ETHUSDT";
            if (sym.contains("SOL")) return "SOLUSDT";
            if (sym.contains("XRP")) return "XRPUSDT";
            if (sym.contains("HYPE")) return "HYPEUSDT";
            if (sym.contains("DOGE")) return "DOGEUSDT";
            if (sym.contains("BNB")) return "BNBUSDT";
        }
        return null;
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
