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
 * WhiteBIT HTTP Fetcher。futures 只拉一次，现货 ticker 按 symbol 并发，避免超 2s TTL。
 * API: https://docs.whitebit.com/
 */
public class WhiteBITFetcher implements HttpExchangeFetcher {

    private static final String FUTURES_URL = "https://whitebit.com/api/v4/public/futures";
    private static final String TICKER_URL = "https://whitebit.com/api/v4/public/ticker";

    private static final List<SpotPair> SPOT_PAIRS = List.of(
            new SpotPair("BTC_USDT", "BTCUSDT"), new SpotPair("ETH_USDT", "ETHUSDT"),
            new SpotPair("SOL_USDT", "SOLUSDT"), new SpotPair("XRP_USDT", "XRPUSDT"),
            new SpotPair("DOGE_USDT", "DOGEUSDT"), new SpotPair("BNB_USDT", "BNBUSDT"));
    private static final Map<String, String> PERP_TO_STD = Map.of(
            "BTC_PERP", "BTCUSDT", "ETH_PERP", "ETHUSDT", "SOL_PERP", "SOLUSDT",
            "XRP_PERP", "XRPUSDT", "DOGE_PERP", "DOGEUSDT", "BNB_PERP", "BNBUSDT");

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public WhiteBITFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "whitebit";
    }

    @Override
    public void fetchAndSave() {
        String futuresJson = null;
        try {
            futuresJson = restTemplate.getForObject(FUTURES_URL, String.class);
        } catch (Exception ignored) {}
        if (futuresJson != null) {
            parseFuturesAndSave(futuresJson);
        }
        var futures = SPOT_PAIRS.stream()
                .map(p -> executor.submit(() -> fetchSpotTicker(p.whitebitSymbol, p.stdSymbol)))
                .toList();
        for (var f : futures) {
            try { f.get(); } catch (Exception ignored) {}
        }
    }

    private void parseFuturesAndSave(String futuresJson) {
        try {
            JsonNode root = objectMapper.readTree(futuresJson);
            JsonNode result = root.has("result") ? root.get("result") : root;
            if (!result.isArray()) return;
            for (JsonNode item : result) {
                String tickerId = item.has("ticker_id") ? item.get("ticker_id").asText() : null;
                String stdSymbol = tickerId != null ? PERP_TO_STD.get(tickerId) : null;
                if (stdSymbol == null) continue;
                BigDecimal fundingRate = parseDecimal(item, "funding_rate");
                Long nextFundingTime = null;
                if (item.has("next_funding_rate_timestamp")) {
                    try {
                        nextFundingTime = Long.parseLong(item.get("next_funding_rate_timestamp").asText());
                    } catch (NumberFormatException ignored) {}
                }
                if (fundingRate != null) marketDataService.saveFundingRate("whitebit", stdSymbol, fundingRate, nextFundingTime);
                BigDecimal lastPrice = parseDecimal(item, "last_price");
                if (lastPrice == null) lastPrice = parseDecimal(item, "price");
                if (lastPrice != null) marketDataService.saveFuturesPrice("whitebit", stdSymbol, lastPrice);
            }
        } catch (Exception ignored) {}
    }

    private void fetchSpotTicker(String whitebitSymbol, String stdSymbol) {
        try {
            String url = TICKER_URL + "?market=" + whitebitSymbol;
            String tickerJson = restTemplate.getForObject(url, String.class);
            if (tickerJson != null) {
                JsonNode root = objectMapper.readTree(tickerJson);
                String[] keys = {whitebitSymbol, whitebitSymbol.replace("_", ""), whitebitSymbol.replace("_", "-")};
                for (String key : keys) {
                    if (root.has(key)) {
                        BigDecimal lastPrice = parseDecimal(root.get(key), "last_price");
                        if (lastPrice != null) {
                            marketDataService.saveSpotPrice("whitebit", stdSymbol, lastPrice);
                            break;
                        }
                    }
                }
            }
        } catch (Exception ignored) {}
    }

    private record SpotPair(String whitebitSymbol, String stdSymbol) {}

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
