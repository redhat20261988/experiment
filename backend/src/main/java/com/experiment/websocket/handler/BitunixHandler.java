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
 * Bitunix WebSocket - 永续期货资金费率、期货价格、现货价格。
 * WebSocket: wss://fapi.bitunix.com/public/
 */
public class BitunixHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://fapi.bitunix.com/public/";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BitunixHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("bitunix", URI.create(WS_URL), this);
    }

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("Bitunix WebSocket connected");
        try {
            // Subscribe to ticker - correct format per API docs
            String subscribeTicker = "{\"op\":\"subscribe\",\"args\":[{\"symbol\":\"BTCUSDT\",\"ch\":\"ticker\"},{\"symbol\":\"ETHUSDT\",\"ch\":\"ticker\"},{\"symbol\":\"SOLUSDT\",\"ch\":\"ticker\"},{\"symbol\":\"XRPUSDT\",\"ch\":\"ticker\"},{\"symbol\":\"DOGEUSDT\",\"ch\":\"ticker\"},{\"symbol\":\"BNBUSDT\",\"ch\":\"ticker\"}]}";
            client.send(subscribeTicker);
        } catch (Exception e) {
            log.warn("Bitunix subscribe error: {}", e.getMessage());
        }
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            
            // Bitunix ticker format: {"ch":"ticker","symbol":"BTCUSDT","data":{...}}
            if (root.has("ch") && "ticker".equals(root.get("ch").asText())) {
                String symbol = root.has("symbol") ? root.get("symbol").asText() : null;
                if (symbol == null) return;
                
                String stdSymbol = null;
                if (symbol.contains("BTC")) stdSymbol = "BTCUSDT";
                else if (symbol.contains("ETH")) stdSymbol = "ETHUSDT";
                else if (symbol.contains("SOL")) stdSymbol = "SOLUSDT";
                else if (symbol.contains("XRP")) stdSymbol = "XRPUSDT";
                else if (symbol.contains("DOGE")) stdSymbol = "DOGEUSDT";
                else if (symbol.contains("BNB")) stdSymbol = "BNBUSDT";
                
                if (stdSymbol == null) return;
                JsonNode data = root.has("data") ? root.get("data") : root;
                
                // Parse ticker data - Bitunix uses "la" for last price
                // Bitunix WebSocket只提供期货ticker数据，只保存期货价格
                // 现货价格需要从现货API单独获取
                BigDecimal lastPrice = parseDecimal(data, "la");
                if (lastPrice != null) {
                    marketDataService.saveFuturesPrice("bitunix", stdSymbol, lastPrice);
                }
            } else {
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("Bitunix parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("Bitunix parse error: {}", e.getMessage());
            }
        }
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
