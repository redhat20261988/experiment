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
 * LBank WebSocket - 永续期货资金费率、期货价格、现货价格。
 * WebSocket: wss://lbkperpws.lbank.com/ws
 */
public class LBankHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://lbkperpws.lbank.com/ws";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public LBankHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("lbank", URI.create(WS_URL), this);
    }

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("LBank WebSocket connected");
        try {
            // LBank WebSocket format - try different subscription format
            // Subscribe to ticker for BTC and ETH separately
            String subscribeBtc = "{\"action\":\"subscribe\",\"subscribe\":\"ticker\",\"pair\":\"BTC_USDT\"}";
            client.send(subscribeBtc);
            String subscribeEth = "{\"action\":\"subscribe\",\"subscribe\":\"ticker\",\"pair\":\"ETH_USDT\"}";
            client.send(subscribeEth);
            String subscribeSol = "{\"action\":\"subscribe\",\"subscribe\":\"ticker\",\"pair\":\"SOL_USDT\"}";
            client.send(subscribeSol);
            String subscribeXrp = "{\"action\":\"subscribe\",\"subscribe\":\"ticker\",\"pair\":\"XRP_USDT\"}";
            client.send(subscribeXrp);
            String subscribeDoge = "{\"action\":\"subscribe\",\"subscribe\":\"ticker\",\"pair\":\"DOGE_USDT\"}";
            client.send(subscribeDoge);
            String subscribeBnb = "{\"action\":\"subscribe\",\"subscribe\":\"ticker\",\"pair\":\"BNB_USDT\"}";
            client.send(subscribeBnb);
        } catch (Exception e) {
            log.warn("LBank subscribe error: {}", e.getMessage());
        }
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            
            if (root.has("channel")) {
                String channel = root.get("channel").asText();
                JsonNode data = root.has("data") ? root.get("data") : root;
                
                // 精确匹配主合约，避免 PUMPBTC_USDT/BTCDOM_USDT 等误映射导致异常低价覆盖
                String pair = root.has("pair") ? root.get("pair").asText() : "";
                String pairNorm = pair.toUpperCase().replace("-", "_").replace(" ", "");
                String symbol = null;
                if ("BTC_USDT".equals(pairNorm)) symbol = "BTCUSDT";
                else if ("ETH_USDT".equals(pairNorm)) symbol = "ETHUSDT";
                else if ("SOL_USDT".equals(pairNorm)) symbol = "SOLUSDT";
                else if ("XRP_USDT".equals(pairNorm)) symbol = "XRPUSDT";
                else if ("DOGE_USDT".equals(pairNorm)) symbol = "DOGEUSDT";
                else if ("BNB_USDT".equals(pairNorm)) symbol = "BNBUSDT";
                
                if (symbol == null) return;
                
                if ("funding_rate".equals(channel)) {
                    BigDecimal rate = parseDecimal(data, "fundingRate");
                    if (rate != null) {
                        marketDataService.saveFundingRate("lbank", symbol, rate, null);
                    }
                } else if ("ticker".equals(channel)) {
                    BigDecimal lastPrice = parseDecimal(data, "last");
                    BigDecimal markPrice = parseDecimal(data, "markPrice");
                    BigDecimal indexPrice = parseDecimal(data, "indexPrice");
                    
                    // LBank期货ticker：优先使用markPrice，如果没有则使用lastPrice作为期货价格
                    BigDecimal futuresPrice = markPrice != null ? markPrice : lastPrice;
                    if (futuresPrice != null) {
                        marketDataService.saveFuturesPrice("lbank", symbol, futuresPrice);
                    }
                    
                    // LBank期货ticker的indexPrice是现货价格，如果为null则不保存现货价格
                    // 不应fallback到期货价格（lastPrice），因为会导致价差为0
                    if (indexPrice != null) {
                        marketDataService.saveSpotPrice("lbank", symbol, indexPrice);
                    }
                }
            } else {
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("LBank parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("LBank parse error: {}", e.getMessage());
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
