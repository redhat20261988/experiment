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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Bybit HTTP 兜底 - WebSocket 断连或推送间隔 > 2s 时，资金费率/期货/现货会空，用 REST 补充。
 * API: https://api.bybit.com/v5/market/tickers 一次返回 fundingRate/nextFundingTime/markPrice/indexPrice
 */
public class BybitFetcher implements HttpExchangeFetcher {

    private static final String TICKERS_URL = "https://api.bybit.com/v5/market/tickers?category=linear&symbol=%s";

    private static final List<String> SYMBOLS = List.of("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "DOGEUSDT", "BNBUSDT");

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public BybitFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "bybit";
    }

    @Override
    public void fetchAndSave() {
        var futures = SYMBOLS.stream()
                .map(s -> executor.submit(() -> fetchSymbol(s)))
                .toList();
        for (var f : futures) {
            try { f.get(); } catch (Exception ignored) {}
        }
    }

    private void fetchSymbol(String symbol) {
        try {
            String json = restTemplate.getForObject(String.format(TICKERS_URL, symbol), String.class);
            if (json == null) return;
            JsonNode root = objectMapper.readTree(json);
            if (root.path("retCode").asInt() != 0 || !root.has("result") || !root.get("result").has("list")) return;
            JsonNode list = root.get("result").get("list");
            if (list.size() == 0) return;
            JsonNode item = list.get(0);

            BigDecimal rate = parseDecimal(item, "fundingRate");
            Long nextTime = item.has("nextFundingTime") ? item.get("nextFundingTime").asLong() : null;
            if (rate != null) marketDataService.saveFundingRate("bybit", symbol, rate, nextTime);

            BigDecimal markPrice = parseDecimal(item, "markPrice");
            BigDecimal indexPrice = parseDecimal(item, "indexPrice");
            if (markPrice != null) marketDataService.saveFuturesPrice("bybit", symbol, markPrice);
            if (indexPrice != null) marketDataService.saveSpotPrice("bybit", symbol, indexPrice);
        } catch (Exception e) {
            // Ignore
        }
    }

    private BigDecimal parseDecimal(JsonNode node, String key) {
        if (node == null || !node.has(key)) return null;
        JsonNode n = node.get(key);
        if (n.isNull()) return null;
        try {
            return new BigDecimal(n.asText());
        } catch (Exception e) {
            return null;
        }
    }
}
