package com.experiment.rest.fetcher;

import com.experiment.rest.HttpExchangeFetcher;
import com.experiment.service.MarketDataService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * OKX HTTP 兜底 - WebSocket 断连或推送间隔时用 REST 补充。一次拉 tickers，并发拉 funding。
 */
public class OkxFetcher implements HttpExchangeFetcher {

    private static final String TICKERS_SWAP_URL = "https://www.okx.com/api/v5/market/tickers?instType=SWAP";
    private static final String TICKERS_SPOT_URL = "https://www.okx.com/api/v5/market/tickers?instType=SPOT";
    private static final String FUNDING_URL = "https://www.okx.com/api/v5/public/funding-rate?instId=%s";

    private static final List<String> SYMBOLS = List.of("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "DOGEUSDT", "BNBUSDT");
    private static final Map<String, String> SYMBOL_TO_INST = Map.of(
            "BTCUSDT", "BTC-USDT-SWAP", "ETHUSDT", "ETH-USDT-SWAP", "SOLUSDT", "SOL-USDT-SWAP",
            "XRPUSDT", "XRP-USDT-SWAP", "HYPEUSDT", "HYPE-USDT-SWAP", "DOGEUSDT", "DOGE-USDT-SWAP",
            "BNBUSDT", "BNB-USDT-SWAP");
    private static final Map<String, String> SYMBOL_TO_SPOT = Map.of(
            "BTCUSDT", "BTC-USDT", "ETHUSDT", "ETH-USDT", "SOLUSDT", "SOL-USDT",
            "XRPUSDT", "XRP-USDT", "HYPEUSDT", "HYPE-USDT", "DOGEUSDT", "DOGE-USDT",
            "BNBUSDT", "BNB-USDT");

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public OkxFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "okx";
    }

    @Override
    public void fetchAndSave() {
        String swapJson = null, spotJson = null;
        try {
            swapJson = restTemplate.getForObject(TICKERS_SWAP_URL, String.class);
            spotJson = restTemplate.getForObject(TICKERS_SPOT_URL, String.class);
        } catch (Exception ignored) {}
        parseTickersAndSave(swapJson, spotJson);

        var futures = SYMBOLS.stream()
                .map(s -> executor.submit(() -> fetchFunding(s)))
                .toList();
        for (var f : futures) {
            try { f.get(); } catch (Exception ignored) {}
        }
    }

    private void parseTickersAndSave(String swapJson, String spotJson) {
        try {
            if (swapJson != null) {
                JsonNode root = objectMapper.readTree(swapJson);
                if ("0".equals(root.path("code").asText()) && root.has("data") && root.get("data").isArray()) {
                    for (JsonNode item : root.get("data")) {
                        String instId = item.has("instId") ? item.get("instId").asText() : null;
                        String symbol = instToSymbol(instId);
                        if (symbol == null) continue;
                        BigDecimal last = parseDecimal(item, "last");
                        if (last != null) marketDataService.saveFuturesPrice("okx", symbol, last);
                    }
                }
            }
            if (spotJson != null) {
                JsonNode root = objectMapper.readTree(spotJson);
                if ("0".equals(root.path("code").asText()) && root.has("data") && root.get("data").isArray()) {
                    for (JsonNode item : root.get("data")) {
                        String instId = item.has("instId") ? item.get("instId").asText() : null;
                        String symbol = spotInstToSymbol(instId);
                        if (symbol == null) continue;
                        BigDecimal last = parseDecimal(item, "last");
                        if (last != null) marketDataService.saveSpotPrice("okx", symbol, last);
                    }
                }
            }
        } catch (Exception ignored) {}
    }

    private String instToSymbol(String instId) {
        if (instId == null) return null;
        for (var e : SYMBOL_TO_INST.entrySet()) {
            if (e.getValue().equals(instId)) return e.getKey();
        }
        return null;
    }

    private String spotInstToSymbol(String instId) {
        if (instId == null) return null;
        for (var e : SYMBOL_TO_SPOT.entrySet()) {
            if (e.getValue().equals(instId)) return e.getKey();
        }
        return null;
    }

    private void fetchFunding(String symbol) {
        String instId = SYMBOL_TO_INST.get(symbol);
        if (instId == null) return;
        try {
            String json = restTemplate.getForObject(String.format(FUNDING_URL, instId), String.class);
            if (json == null) return;
            JsonNode root = objectMapper.readTree(json);
            if (!"0".equals(root.path("code").asText()) || !root.has("data") || !root.get("data").isArray()) return;
            JsonNode arr = root.get("data");
            if (arr.isEmpty()) return;
            JsonNode item = arr.get(0);
            BigDecimal rate = parseDecimal(item, "fundingRate");
            Long nextTime = item.has("nextFundingTime") ? item.get("nextFundingTime").asLong() : null;
            if (rate != null) marketDataService.saveFundingRate("okx", symbol, rate, nextTime);
        } catch (Exception ignored) {}
    }

    private BigDecimal parseDecimal(JsonNode node, String key) {
        if (node == null || !node.has(key)) return null;
        try {
            return new BigDecimal(node.get(key).asText());
        } catch (Exception e) {
            return null;
        }
    }
}
