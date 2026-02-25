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

public class BitgetHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://ws.bitget.com/v2/ws/public";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BitgetHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("bitget", URI.create(WS_URL), this);
    }

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("Bitget WebSocket connected");
        // instType "USDT-FUTURES" = 永续合约；"mc" 为现货/杠杆，不含资金费率
        client.send("{\"op\":\"subscribe\",\"args\":[{\"instType\":\"USDT-FUTURES\",\"channel\":\"ticker\",\"instId\":\"BTCUSDT\"},{\"instType\":\"USDT-FUTURES\",\"channel\":\"ticker\",\"instId\":\"ETHUSDT\"},{\"instType\":\"USDT-FUTURES\",\"channel\":\"ticker\",\"instId\":\"SOLUSDT\"},{\"instType\":\"USDT-FUTURES\",\"channel\":\"ticker\",\"instId\":\"XRPUSDT\"},{\"instType\":\"USDT-FUTURES\",\"channel\":\"ticker\",\"instId\":\"HYPEUSDT\"},{\"instType\":\"USDT-FUTURES\",\"channel\":\"ticker\",\"instId\":\"DOGEUSDT\"},{\"instType\":\"USDT-FUTURES\",\"channel\":\"ticker\",\"instId\":\"BNBUSDT\"}]}");
    }

    @Override
    public void onMessage(String message) {
        try {
            if ("pong".equals(message)) return;

            JsonNode root = objectMapper.readTree(message);
            if (root.has("event") && "error".equals(root.get("event").asText())) return;

            JsonNode data = root.get("data");
            if (data == null) return;

            if (data.isArray()) {
                for (JsonNode item : data) processTicker(item);
            } else {
                processTicker(data);
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("Bitget parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("Bitget parse error: {}", e.getMessage());
            }
        }
    }

    @Override
    public String getHeartbeatMessage() {
        return "ping";
    }

    @Override
    public long getHeartbeatIntervalMs() {
        return 25_000;
    }

    private void processTicker(JsonNode item) {
        String instId = item.has("instId") ? item.get("instId").asText() : "";
        if (instId.isEmpty()) return;

        BigDecimal lastPr = parseDecimal(item, "lastPr");
        BigDecimal markPr = parseDecimal(item, "markPr");
        BigDecimal fundingRate = parseDecimal(item, "fundingRate");
        Long nextFundingTime = item.has("nextFundingTime") ? item.get("nextFundingTime").asLong() : null;

        marketDataService.saveFundingRate("bitget", instId, fundingRate, nextFundingTime);
        marketDataService.saveFuturesPrice("bitget", instId, markPr != null ? markPr : lastPr);
        // Bitget期货ticker只提供期货价格，不保存现货价格
        // 现货价格需要从现货API单独获取
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
