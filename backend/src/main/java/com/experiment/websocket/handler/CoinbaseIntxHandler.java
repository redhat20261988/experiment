package com.experiment.websocket.handler;

import com.experiment.service.MarketDataService;
import com.experiment.util.RedisShutdownUtil;
import com.experiment.websocket.ExchangeWebSocketHandler;
import com.experiment.websocket.ManagedWebSocket;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Coinbase 国际站 (INTX) WebSocket - 永续期货 FUNDING、RISK 频道。
 * 端点: wss://ws-cloud.international.coinbase.com
 * 需配置 API Key 认证，未配置时不启动。
 */
@Component
public class CoinbaseIntxHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://ws-cloud.international.coinbase.com";
    private static final String AUTH_PAYLOAD = "CBINTLMD";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${coinbase.intx.api-key:}")
    private String apiKey;

    @Value("${coinbase.intx.api-secret:}")
    private String apiSecret;

    @Value("${coinbase.intx.passphrase:}")
    private String passphrase;

    public CoinbaseIntxHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public boolean isConfigured() {
        boolean apiKeySet = apiKey != null && !apiKey.isBlank();
        boolean apiSecretSet = apiSecret != null && !apiSecret.isBlank();
        boolean passphraseSet = passphrase != null && !passphrase.isBlank();
        return apiKeySet && apiSecretSet && passphraseSet;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("coinbase-intx", URI.create(WS_URL), this);
    }

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("Coinbase INTX WebSocket connected");
        String time = String.valueOf(System.currentTimeMillis() / 1000);
        String signature = sign(time, apiKey, passphrase, apiSecret);
        ObjectNode subscribe = objectMapper.createObjectNode();
        subscribe.put("type", "SUBSCRIBE");
        subscribe.putArray("product_ids").add("BTC-PERP").add("ETH-PERP");
        subscribe.putArray("channels").add("FUNDING").add("RISK");
        subscribe.put("time", time);
        subscribe.put("key", apiKey);
        subscribe.put("passphrase", passphrase);
        subscribe.put("signature", signature);
        try {
            String subscribeMsg = objectMapper.writeValueAsString(subscribe);
            client.send(subscribeMsg);
        } catch (Exception e) {
            log.warn("Coinbase INTX subscribe error: {}", e.getMessage());
        }
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            String channel = root.has("channel") ? root.get("channel").asText() : "";
            String productId = root.has("product_id") ? root.get("product_id").asText() : "";
            String symbol = toStdSymbol(productId);
            if (symbol == null) return;

            if ("FUNDING".equals(channel)) {
                BigDecimal rate = parseDecimal(root, "funding_rate");
                if (rate != null) {
                    // is_final=true 为上一周期最终费率，false 为当前周期预测费率，均可展示
                    // FUNDING 消息不含下次结算时间，传 null
                    marketDataService.saveFundingRate("coinbase", symbol, rate, null);
                }
            } else if ("RISK".equals(channel)) {
                BigDecimal markPrice = parseDecimal(root, "mark_price");
                BigDecimal indexPrice = parseDecimal(root, "index_price");
                if (markPrice != null) {
                    marketDataService.saveFuturesPrice("coinbase", symbol, markPrice);
                }
                if (indexPrice != null) marketDataService.saveSpotPrice("coinbase", symbol, indexPrice);
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("Coinbase INTX parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("Coinbase INTX parse error: {}", e.getMessage());
            }
        }
    }

    private String toStdSymbol(String productId) {
        if (productId == null) return null;
        if (productId.startsWith("BTC")) return "BTCUSDT";
        if (productId.startsWith("ETH")) return "ETHUSDT";
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

    private static String sign(String timestamp, String key, String passphrase, String secret) {
        try {
            String payload = timestamp + key + AUTH_PAYLOAD + passphrase;
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] hash = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            throw new RuntimeException("INTX signature failed", e);
        }
    }
}
