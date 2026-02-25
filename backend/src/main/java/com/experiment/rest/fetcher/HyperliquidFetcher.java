package com.experiment.rest.fetcher;

import com.experiment.rest.HttpExchangeFetcher;
import com.experiment.service.MarketDataService;
import com.experiment.util.RedisShutdownUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Hyperliquid HTTP Fetcher - 资金费率（WebSocket只提供价格，资金费率需HTTP API）。
 * API: https://hyperliquid.gitbook.io/hyperliquid-docs/
 */
public class HyperliquidFetcher implements HttpExchangeFetcher {

    private static final String INFO_URL = "https://api.hyperliquid.xyz/info";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public HyperliquidFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "hyperliquid";
    }

    private static final int RETRY_COUNT = 5;
    private static final long RETRY_DELAY_MS = 300;

    /** 带重试的 HTTP POST，应对间歇性网络/SSL 失败 */
    private String postWithRetry(HttpEntity<?> entity) {
        Exception last = null;
        for (int i = 0; i < RETRY_COUNT; i++) {
            try {
                return restTemplate.postForObject(INFO_URL, entity, String.class);
            } catch (Exception e) {
                last = e;
                if (i < RETRY_COUNT - 1) {
                    try { Thread.sleep(RETRY_DELAY_MS); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return null; }
                }
            }
        }
        if (last != null) throw last instanceof RuntimeException r ? r : new RuntimeException(last);
        return null;
    }

    @Override
    public void fetchAndSave() {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("type", "metaAndAssetCtxs");
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);
            String responseJson = postWithRetry(request);
            
            if (responseJson != null) {
                JsonNode root = objectMapper.readTree(responseJson);
                
                // Hyperliquid API returns an array: [meta, assetCtxs]
                // meta[0] = {universe: [...], marginTables: [...], collateralToken: ...}
                // assetCtxs[1] = [{funding: "...", ...}, ...]
                // The assetCtxs array corresponds to universe array by index
                if (root.isArray() && root.size() >= 2) {
                    JsonNode meta = root.get(0);
                    JsonNode assetCtxs = root.get(1);
                    
                    if (meta.has("universe") && assetCtxs.isArray()) {
                        JsonNode universe = meta.get("universe");
                        
                        // Find BTC, ETH, SOL, XRP, HYPE, DOGE, BNB indices in universe
                        String[] coinNames = {"BTC", "ETH", "SOL", "XRP", "HYPE", "DOGE", "BNB"};
                        String[] symbols = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "DOGEUSDT", "BNBUSDT"};
                        int[] indices = new int[coinNames.length];
                        for (int i = 0; i < indices.length; i++) {
                            indices[i] = -1;
                        }
                        
                        for (int i = 0; i < universe.size(); i++) {
                            JsonNode asset = universe.get(i);
                            if (asset.has("name")) {
                                String name = asset.get("name").asText();
                                for (int j = 0; j < coinNames.length; j++) {
                                    if (coinNames[j].equals(name)) {
                                        indices[j] = i;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        // Extract funding and spot from same metaAndAssetCtxs (一次请求同时获取，减少空数据)
                        for (int j = 0; j < coinNames.length; j++) {
                            if (indices[j] >= 0 && indices[j] < assetCtxs.size()) {
                                JsonNode ctx = assetCtxs.get(indices[j]);
                                BigDecimal rate = parseFundingRate(ctx);
                                if (rate != null) marketDataService.saveFundingRate("hyperliquid", symbols[j], rate, null);
                                BigDecimal spot = parseSpotPrice(ctx);
                                if (spot != null) marketDataService.saveSpotPrice("hyperliquid", symbols[j], spot);
                            }
                        }
                    }
                }
            }
            // 不再调用 spotMetaAndAssetCtxs / fetchSpotPricesFromPerps：spot universe 无 BTC/ETH，
            // 且 metaAndAssetCtxs 已提供 funding + spot（perp markPx/oraclePx）。单次请求可显著缩短耗时，避免 TTL 空窗
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("[hyperliquid] fetch skipped (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("[hyperliquid] fetch funding rate error: {}", e.getMessage(), e);
            }
        }
    }
    
    private BigDecimal parseSpotPrice(JsonNode assetCtx) {
        if (assetCtx == null) {
            return null;
        }
        BigDecimal price = parseDecimal(assetCtx, "markPx");
        if (price == null) price = parseDecimal(assetCtx, "midPx");
        if (price == null) price = parseDecimal(assetCtx, "oraclePx");
        if (price == null) price = parseDecimal(assetCtx, "prevDayPx");
        return price;
    }
    
    private BigDecimal parseDecimal(JsonNode node, String key) {
        if (node == null || !node.has(key)) {
            return null;
        }
        JsonNode n = node.get(key);
        if (n.isNull()) {
            return null;
        }
        try {
            if (n.isTextual()) {
                String s = n.asText();
                if (s != null && !s.isEmpty()) {
                    return new BigDecimal(s);
                }
            } else if (n.isNumber()) {
                return n.decimalValue();
            }
        } catch (NumberFormatException e) {
            // Ignore parse errors
        }
        return null;
    }
    
    private BigDecimal parseFundingRate(JsonNode assetCtx) {
        if (assetCtx == null || !assetCtx.has("funding")) {
            return null;
        }
        
        JsonNode fundingNode = assetCtx.get("funding");
        if (fundingNode.isNull()) {
            return null;
        }
        
        try {
            if (fundingNode.isTextual()) {
                String fundingStr = fundingNode.asText();
                if (fundingStr != null && !fundingStr.isEmpty()) {
                    return new BigDecimal(fundingStr);
                }
            } else if (fundingNode.isNumber()) {
                return fundingNode.decimalValue();
            }
        } catch (NumberFormatException e) {
            // Ignore parse errors
        }
        
        return null;
    }
}
