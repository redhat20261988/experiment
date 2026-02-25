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

public class OkxHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://ws.okx.com:8443/ws/v5/public";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OkxHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("okx", URI.create(WS_URL), this);
    }

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("OKX WebSocket connected");
        String subscribe = """
                {"op":"subscribe","args":[
                    {"channel":"funding-rate","instId":"BTC-USDT-SWAP"},
                    {"channel":"funding-rate","instId":"ETH-USDT-SWAP"},
                    {"channel":"funding-rate","instId":"SOL-USDT-SWAP"},
                    {"channel":"funding-rate","instId":"XRP-USDT-SWAP"},
                    {"channel":"funding-rate","instId":"HYPE-USDT-SWAP"},
                    {"channel":"funding-rate","instId":"DOGE-USDT-SWAP"},
                    {"channel":"funding-rate","instId":"BNB-USDT-SWAP"},
                    {"channel":"tickers","instId":"BTC-USDT-SWAP"},
                    {"channel":"tickers","instId":"ETH-USDT-SWAP"},
                    {"channel":"tickers","instId":"SOL-USDT-SWAP"},
                    {"channel":"tickers","instId":"XRP-USDT-SWAP"},
                    {"channel":"tickers","instId":"HYPE-USDT-SWAP"},
                    {"channel":"tickers","instId":"DOGE-USDT-SWAP"},
                    {"channel":"tickers","instId":"BNB-USDT-SWAP"},
                    {"channel":"tickers","instId":"BTC-USDT"},
                    {"channel":"tickers","instId":"ETH-USDT"},
                    {"channel":"tickers","instId":"SOL-USDT"},
                    {"channel":"tickers","instId":"XRP-USDT"},
                    {"channel":"tickers","instId":"HYPE-USDT"},
                    {"channel":"tickers","instId":"DOGE-USDT"},
                    {"channel":"tickers","instId":"BNB-USDT"}
                ]}
                """;
        client.send(subscribe);
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            if (root.has("event")) return;
            JsonNode arg = root.get("arg");
            if (arg == null) return;
            String channel = arg.has("channel") ? arg.get("channel").asText() : "";
            String instId = arg.has("instId") ? arg.get("instId").asText() : "";
            String symbol = "";
            if (instId.contains("BTC")) symbol = "BTCUSDT";
            else if (instId.contains("ETH")) symbol = "ETHUSDT";
            else if (instId.contains("SOL")) symbol = "SOLUSDT";
            else if (instId.contains("XRP")) symbol = "XRPUSDT";
            else if (instId.contains("HYPE")) symbol = "HYPEUSDT";
            else if (instId.contains("DOGE")) symbol = "DOGEUSDT";
            else if (instId.contains("BNB")) symbol = "BNBUSDT";
            
            // 如果symbol为空，说明instId不是支持的币种，跳过处理
            if (symbol.isEmpty()) return;

            JsonNode data = root.get("data");
            if (data == null || !data.isArray() || data.isEmpty()) return;
            JsonNode item = data.get(0);

            if ("funding-rate".equals(channel)) {
                BigDecimal rate = parseDecimal(item, "fundingRate");
                Long nextFundingTime = item.has("nextFundingTime") ? item.get("nextFundingTime").asLong() : null;
                marketDataService.saveFundingRate("okx", symbol, rate, nextFundingTime);
            } else if ("tickers".equals(channel)) {
                BigDecimal last = parseDecimal(item, "last");
                if (instId.contains("SWAP")) {
                    marketDataService.saveFuturesPrice("okx", symbol, last);
                } else {
                    marketDataService.saveSpotPrice("okx", symbol, last);
                }
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("OKX parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("OKX parse error: {}", e.getMessage());
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
