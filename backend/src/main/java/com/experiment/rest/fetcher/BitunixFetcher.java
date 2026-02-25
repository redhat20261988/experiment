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
 * Bitunix HTTP Fetcher - 资金费率、期货、现货。与 WebSocket 互为兜底，并发拉取避免 2s TTL 下先写数据过期。
 * API: https://openapidoc.bitunix.com/
 */
public class BitunixFetcher implements HttpExchangeFetcher {

    private static final String FUNDING_URL = "https://fapi.bitunix.com/api/v1/futures/market/funding_rate?symbol=%s";
    private static final String SPOT_PRICE_URL = "https://openapi.bitunix.com/api/spot/v1/market/last_price?symbol=%s";
    private static final List<SymbolPair> SYMBOLS = List.of(
            new SymbolPair("BTCUSDT", "BTCUSDT"), new SymbolPair("ETHUSDT", "ETHUSDT"),
            new SymbolPair("SOLUSDT", "SOLUSDT"), new SymbolPair("XRPUSDT", "XRPUSDT"),
            new SymbolPair("DOGEUSDT", "DOGEUSDT"), new SymbolPair("BNBUSDT", "BNBUSDT"));

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public BitunixFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "bitunix";
    }

    @Override
    public void fetchAndSave() {
        var futures = SYMBOLS.stream()
                .map(p -> executor.submit(() -> fetchSymbol(p.symbol, p.stdSymbol)))
                .toList();
        for (var f : futures) {
            try { f.get(); } catch (Exception ignored) {}
        }
    }

    private void fetchSymbol(String bitunixSymbol, String stdSymbol) {
        try {
            // 获取资金费率
            String url = String.format(FUNDING_URL, bitunixSymbol);
            String fundingJson = restTemplate.getForObject(url, String.class);
            if (fundingJson != null) {
                JsonNode root = objectMapper.readTree(fundingJson);
                if (root.has("code") && root.get("code").asInt() == 0) {
                    JsonNode data = root.get("data");
                    if (data != null) {
                        BigDecimal rate = parseDecimal(data, "fundingRate");
                        if (rate != null) {
                            Long nextTime = data.has("nextFundingTime") ? parseLong(data, "nextFundingTime") : null;
                            marketDataService.saveFundingRate("bitunix", stdSymbol, rate, nextTime);
                        }
                        BigDecimal markPrice = parseDecimal(data, "markPrice");
                        BigDecimal lastPrice = parseDecimal(data, "lastPrice");
                        BigDecimal futuresPrice = markPrice != null ? markPrice : lastPrice;
                        if (futuresPrice != null) {
                            marketDataService.saveFuturesPrice("bitunix", stdSymbol, futuresPrice);
                        }
                    }
                }
            }
            
            // 获取现货价格
            String spotUrl = String.format(SPOT_PRICE_URL, bitunixSymbol);
            String spotJson = restTemplate.getForObject(spotUrl, String.class);
            if (spotJson != null) {
                JsonNode root = objectMapper.readTree(spotJson);
                if (root.has("code") && root.get("code").asInt() == 0) {
                    JsonNode data = root.get("data");
                    if (data != null) {
                        BigDecimal spotPrice = parseSpotPriceFromData(data);
                        if (spotPrice != null) {
                            marketDataService.saveSpotPrice("bitunix", stdSymbol, spotPrice);
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Ignore fetch errors
        }
    }

    /** data 可能为字符串（如 "64839.42"）或对象（含 price/last）。 */
    private BigDecimal parseSpotPriceFromData(JsonNode data) {
        if (data == null) return null;
        if (data.isTextual()) {
            String s = data.asText();
            if (s == null || s.isEmpty()) return null;
            try {
                return new BigDecimal(s);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        BigDecimal p = parseDecimal(data, "price");
        if (p != null) return p;
        return parseDecimal(data, "last");
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

    private Long parseLong(JsonNode node, String key) {
        if (!node.has(key)) return null;
        JsonNode n = node.get(key);
        if (n.isNull()) return null;
        try {
            return n.asLong();
        } catch (Exception e) {
            return null;
        }
    }

    private record SymbolPair(String symbol, String stdSymbol) {}
}
