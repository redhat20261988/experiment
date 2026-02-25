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
 * HTX 纯 HTTP。7 交易对 × 3 请求串行易超 2s TTL，改为并发拉取。
 */
public class HtxFetcher implements HttpExchangeFetcher {

    private static final String FUNDING_URL = "https://api.hbdm.com/linear-swap-api/v1/swap_funding_rate?contract_code=%s";
    private static final String MERGED_URL = "https://api.hbdm.com/linear-swap-ex/market/detail/merged?contract_code=%s";
    private static final String INDEX_URL = "https://api.hbdm.com/linear-swap-api/v1/swap_index?contract_code=%s";

    private static final List<SymbolPair> SYMBOLS = List.of(
            new SymbolPair("BTC-USDT", "BTCUSDT"), new SymbolPair("ETH-USDT", "ETHUSDT"),
            new SymbolPair("SOL-USDT", "SOLUSDT"), new SymbolPair("XRP-USDT", "XRPUSDT"),
            new SymbolPair("HYPE-USDT", "HYPEUSDT"), new SymbolPair("DOGE-USDT", "DOGEUSDT"),
            new SymbolPair("BNB-USDT", "BNBUSDT"));

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public HtxFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "htx";
    }

    @Override
    public void fetchAndSave() {
        var futures = SYMBOLS.stream()
                .map(p -> executor.submit(() -> fetchSymbol(p.contractCode, p.stdSymbol)))
                .toList();
        for (var f : futures) {
            try { f.get(); } catch (Exception ignored) {}
        }
    }

    private void fetchSymbol(String contractCode, String stdSymbol) {
        try {
            String fundingJson = restTemplate.getForObject(String.format(FUNDING_URL, contractCode), String.class);
            if (fundingJson != null) {
                JsonNode root = objectMapper.readTree(fundingJson);
                if ("ok".equals(root.path("status").asText())) {
                    JsonNode data = root.get("data");
                    BigDecimal rate = new BigDecimal(data.path("funding_rate").asText());
                    Long nextTime = data.has("funding_time") && !data.get("funding_time").isNull()
                            ? data.get("funding_time").asLong() : null;
                    marketDataService.saveFundingRate("htx", stdSymbol, rate, nextTime);
                }
            }

            String mergedJson = restTemplate.getForObject(String.format(MERGED_URL, contractCode), String.class);
            if (mergedJson != null) {
                JsonNode root = objectMapper.readTree(mergedJson);
                if ("ok".equals(root.path("status").asText())) {
                    JsonNode tick = root.path("tick");
                    BigDecimal close = new BigDecimal(tick.path("close").asText());
                    marketDataService.saveFuturesPrice("htx", stdSymbol, close);
                }
            }

            String indexJson = restTemplate.getForObject(String.format(INDEX_URL, contractCode), String.class);
            if (indexJson != null) {
                JsonNode root = objectMapper.readTree(indexJson);
                if ("ok".equals(root.path("status").asText())) {
                    JsonNode data = root.get("data");
                    if (data.isArray() && data.size() > 0) {
                        BigDecimal indexPrice = new BigDecimal(data.get(0).path("index_price").asText());
                        marketDataService.saveSpotPrice("htx", stdSymbol, indexPrice);
                    }
                }
            }
        } catch (Exception e) {
            // Ignore fetch errors
        }
    }

    private record SymbolPair(String contractCode, String stdSymbol) {}
}
