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
 * CoinW HTTP Fetcher。并发拉取各交易对，现货 ticker 只拉一次，避免 2s TTL 下数据过期。
 */
public class CoinwFetcher implements HttpExchangeFetcher {

    private static final String FUNDING_URL = "https://api.coinw.com/v1/perpum/fundingRate?instrument=%s";
    private static final String DEPTH_URL = "https://api.coinw.com/v1/perpumPublic/depth?base=%s";
    private static final String SPOT_TICKER_URL = "https://api.coinw.com/api/v1/public?command=returnTicker";

    private static final List<SymbolPair> SYMBOLS = List.of(
            new SymbolPair("btc", "BTCUSDT"), new SymbolPair("eth", "ETHUSDT"),
            new SymbolPair("sol", "SOLUSDT"), new SymbolPair("xrp", "XRPUSDT"),
            new SymbolPair("doge", "DOGEUSDT"), new SymbolPair("bnb", "BNBUSDT"));

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public CoinwFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "coinw";
    }

    @Override
    public void fetchAndSave() {
        Map<String, BigDecimal> spotPrices = fetchSpotPrices();
        var futures = SYMBOLS.stream()
                .map(p -> executor.submit(() -> fetchSymbol(p.instrument, p.stdSymbol, spotPrices)))
                .toList();
        for (var f : futures) {
            try { f.get(); } catch (Exception ignored) {}
        }
    }

    private Map<String, BigDecimal> fetchSpotPrices() {
        try {
            String json = restTemplate.getForObject(SPOT_TICKER_URL, String.class);
            if (json == null) return Map.of();
            JsonNode root = objectMapper.readTree(json);
            JsonNode data = root.has("data") ? root.get("data") : root;
            Map<String, BigDecimal> map = new java.util.HashMap<>();
            for (SymbolPair p : SYMBOLS) {
                String key = p.instrument.toUpperCase() + "_USDT";
                if (data.has(key)) {
                    JsonNode t = data.get(key);
                    BigDecimal price = parseDecimal(t, "last");
                    if (price == null) price = parseDecimal(t, "price");
                    if (price != null) map.put(p.stdSymbol, price);
                }
            }
            return map;
        } catch (Exception e) {
            return Map.of();
        }
    }

    private void fetchSymbol(String instrument, String stdSymbol, Map<String, BigDecimal> spotPrices) {
        try {
            String fundingJson = restTemplate.getForObject(String.format(FUNDING_URL, instrument), String.class);
            if (fundingJson != null) {
                JsonNode root = objectMapper.readTree(fundingJson);
                if (root.path("code").asInt() == 0 && root.has("data")) {
                    JsonNode data = root.get("data");
                    BigDecimal rate = new BigDecimal(data.path("value").asText());
                    marketDataService.saveFundingRate("coinw", stdSymbol, rate, null);
                }
            }

            String depthJson = restTemplate.getForObject(String.format(DEPTH_URL, instrument.toUpperCase()), String.class);
            if (depthJson != null) {
                JsonNode root = objectMapper.readTree(depthJson);
                if (root.path("code").asInt() == 0 && root.has("data")) {
                    JsonNode data = root.get("data");
                    JsonNode bids = data.get("bids");
                    JsonNode asks = data.get("asks");
                    if (bids != null && bids.size() > 0 && asks != null && asks.size() > 0) {
                        BigDecimal bid = new BigDecimal(bids.get(0).path("p").asText());
                        BigDecimal ask = new BigDecimal(asks.get(0).path("p").asText());
                        BigDecimal mid = bid.add(ask).divide(BigDecimal.valueOf(2));
                        marketDataService.saveFuturesPrice("coinw", stdSymbol, mid);
                    }
                }
            }

            BigDecimal spotPrice = spotPrices.get(stdSymbol);
            if (spotPrice != null) {
                marketDataService.saveSpotPrice("coinw", stdSymbol, spotPrice);
            }
        } catch (Exception e) {
            // Ignore fetch errors
        }
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

    private record SymbolPair(String instrument, String stdSymbol) {}
}
