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
 * CoinEx HTTP Fetcher - 资金费率、期货价格、现货价格兜底（原仅 WebSocket，配合 2s Redis TTL 每秒刷新）。
 * Funding+Mark: https://api.coinex.com/v2/futures/funding-rate?market=...
 * Spot: https://api.coinex.com/v2/spot/ticker?market=...
 */
public class CoinExFetcher implements HttpExchangeFetcher {

    private static final String FUNDING_URL = "https://api.coinex.com/v2/futures/funding-rate?market=%s";
    private static final String SPOT_TICKER_URL = "https://api.coinex.com/v2/spot/ticker?market=%s";
    private static final List<String> SYMBOLS = List.of("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "DOGEUSDT", "BNBUSDT");

    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public CoinExFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "coinex";
    }

    @Override
    public void fetchAndSave() {
        String marketList = String.join(",", SYMBOLS);
        executor.submit(() -> fetchFundingAndFutures(marketList));
        for (String symbol : SYMBOLS) {
            executor.submit(() -> fetchSpotPrice(symbol));
        }
    }

    private void fetchFundingAndFutures(String marketList) {
        try {
            String url = String.format(FUNDING_URL, marketList);
            String json = restTemplate.getForObject(url, String.class);
            if (json == null) return;
            JsonNode root = objectMapper.readTree(json);
            if (root.path("code").asInt() != 0 || !root.has("data")) return;
            JsonNode data = root.get("data");
            if (!data.isArray()) return;
            for (JsonNode item : data) {
                String market = item.has("market") ? item.get("market").asText() : null;
                if (market == null || !SYMBOLS.contains(market)) continue;
                BigDecimal rate = parseDecimal(item, "latest_funding_rate");
                Long nextTime = item.has("next_funding_time") ? item.get("next_funding_time").asLong() : null;
                if (rate != null) marketDataService.saveFundingRate("coinex", market, rate, nextTime);
                BigDecimal markPrice = parseDecimal(item, "mark_price");
                if (markPrice != null) marketDataService.saveFuturesPrice("coinex", market, markPrice);
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    private void fetchSpotPrice(String symbol) {
        try {
            String url = String.format(SPOT_TICKER_URL, symbol);
            String json = restTemplate.getForObject(url, String.class);
            if (json == null) return;
            JsonNode root = objectMapper.readTree(json);
            if (root.path("code").asInt() != 0 || !root.has("data")) return;
            JsonNode data = root.get("data");
            if (!data.isArray() || data.size() == 0) return;
            JsonNode ticker = data.get(0);
            BigDecimal last = parseDecimal(ticker, "last");
            if (last == null) last = parseDecimal(ticker, "close");
            if (last != null) marketDataService.saveSpotPrice("coinex", symbol, last);
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
