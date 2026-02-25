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
 * MEXC HTTP 兜底 - 仅 WebSocket 时断连导致空数据，用 REST 补充。
 * API: https://www.mexc.com/api-docs/futures/market-endpoints
 */
public class MexcFetcher implements HttpExchangeFetcher {

    private static final String BASE_URL = "https://api.mexc.com/api/v1/contract";
    private static final String FUNDING_URL = BASE_URL + "/funding_rate/%s";
    private static final String FAIR_PRICE_URL = BASE_URL + "/fair_price/%s";
    private static final String INDEX_PRICE_URL = BASE_URL + "/index_price/%s";

    private static final List<SymbolPair> SYMBOLS = List.of(
            new SymbolPair("BTC_USDT", "BTCUSDT"), new SymbolPair("ETH_USDT", "ETHUSDT"),
            new SymbolPair("SOL_USDT", "SOLUSDT"), new SymbolPair("XRP_USDT", "XRPUSDT"),
            new SymbolPair("HYPE_USDT", "HYPEUSDT"), new SymbolPair("DOGE_USDT", "DOGEUSDT"),
            new SymbolPair("BNB_USDT", "BNBUSDT"));

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public MexcFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "mexc";
    }

    @Override
    public void fetchAndSave() {
        var futures = SYMBOLS.stream()
                .map(p -> executor.submit(() -> fetchSymbol(p.mexcSymbol, p.stdSymbol)))
                .toList();
        for (var f : futures) {
            try { f.get(); } catch (Exception ignored) {}
        }
    }

    private void fetchSymbol(String mexcSymbol, String stdSymbol) {
        try {
            String fundingJson = restTemplate.getForObject(String.format(FUNDING_URL, mexcSymbol), String.class);
            if (fundingJson != null) {
                JsonNode root = objectMapper.readTree(fundingJson);
                if (root.path("code").asInt() == 0 && root.has("data")) {
                    JsonNode d = root.get("data");
                    BigDecimal rate = parseDecimal(d, "fundingRate");
                    Long nextTime = d.has("nextSettleTime") ? d.get("nextSettleTime").asLong() : null;
                    if (rate != null) marketDataService.saveFundingRate("mexc", stdSymbol, rate, nextTime);
                }
            }

            String fairJson = restTemplate.getForObject(String.format(FAIR_PRICE_URL, mexcSymbol), String.class);
            if (fairJson != null) {
                JsonNode root = objectMapper.readTree(fairJson);
                if (root.path("code").asInt() == 0 && root.has("data")) {
                    BigDecimal fair = parseDecimal(root.get("data"), "fairPrice");
                    if (fair != null) marketDataService.saveFuturesPrice("mexc", stdSymbol, fair);
                }
            }

            String indexJson = restTemplate.getForObject(String.format(INDEX_PRICE_URL, mexcSymbol), String.class);
            if (indexJson != null) {
                JsonNode root = objectMapper.readTree(indexJson);
                if (root.path("code").asInt() == 0 && root.has("data")) {
                    BigDecimal index = parseDecimal(root.get("data"), "indexPrice");
                    if (index != null) marketDataService.saveSpotPrice("mexc", stdSymbol, index);
                }
            }
        } catch (Exception ignored) {}
    }

    private BigDecimal parseDecimal(JsonNode node, String key) {
        if (node == null || !node.has(key)) return null;
        JsonNode n = node.get(key);
        if (n == null || n.isNull()) return null;
        try {
            return new BigDecimal(n.asText());
        } catch (Exception e) {
            return null;
        }
    }

    private record SymbolPair(String mexcSymbol, String stdSymbol) {}
}
