package com.experiment.rest.fetcher;

import com.experiment.rest.HttpExchangeFetcher;
import com.experiment.service.MarketDataService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Binance HTTP Fetcher - 资金费率、期货价格、现货价格兜底（原仅 WebSocket，配合 2s Redis TTL 每秒刷新）。
 * API: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT
 */
public class BinanceFetcher implements HttpExchangeFetcher {

    private static final String PREMIUM_INDEX_URL = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=%s";
    private static final List<String> SYMBOLS = List.of("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "DOGEUSDT", "BNBUSDT");

    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public BinanceFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "binance";
    }

    @Override
    public void fetchAndSave() {
        for (String symbol : SYMBOLS) {
            executor.submit(() -> fetchSymbol(symbol));
        }
    }

    private void fetchSymbol(String symbol) {
        try {
            String url = String.format(PREMIUM_INDEX_URL, symbol);
            String json = restTemplate.getForObject(url, String.class);
            if (json == null) return;
            JsonNode root = objectMapper.readTree(json);
            if (root.has("code")) return;
            BigDecimal rate = parseDecimal(root, "lastFundingRate");
            Long nextTime = root.has("nextFundingTime") ? root.get("nextFundingTime").asLong() : null;
            if (rate != null) marketDataService.saveFundingRate("binance", symbol, rate, nextTime);
            BigDecimal markPrice = parseDecimal(root, "markPrice");
            BigDecimal indexPrice = parseDecimal(root, "indexPrice");
            if (markPrice != null) marketDataService.saveFuturesPrice("binance", symbol, markPrice);
            if (indexPrice != null) marketDataService.saveSpotPrice("binance", symbol, indexPrice);
        } catch (Exception e) {
            // Ignore
        }
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
