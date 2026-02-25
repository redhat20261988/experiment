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
 * BingX 纯 HTTP 拉取。7 个交易对 × 2 请求 = 14 次串行会超过 2 秒 Redis TTL，
 * 导致先写入的数据在轮询完成前过期。改为并发拉取，缩短周期至约 0.5s 内。
 */
public class BingxFetcher implements HttpExchangeFetcher {

    private static final String FUNDING_URL = "https://open-api.bingx.com/openApi/swap/v2/quote/fundingRate?symbol=%s";
    /** premiumIndex 返回当前 markPrice（期货）与 indexPrice（现货参考）；swap/quote/price 是永续成交价非现货 */
    private static final String PREMIUM_INDEX_URL = "https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex?symbol=%s";
    private static final String SPOT_PRICE_URL = "https://open-api.bingx.com/openApi/spot/v2/ticker/price?symbol=%s";

    private static final List<SymbolPair> SYMBOLS = List.of(
            new SymbolPair("BTC-USDT", "BTCUSDT"),
            new SymbolPair("ETH-USDT", "ETHUSDT"),
            new SymbolPair("SOL-USDT", "SOLUSDT"),
            new SymbolPair("XRP-USDT", "XRPUSDT"),
            new SymbolPair("HYPE-USDT", "HYPEUSDT"),
            new SymbolPair("DOGE-USDT", "DOGEUSDT"),
            new SymbolPair("BNB-USDT", "BNBUSDT")
    );

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public BingxFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "bingx";
    }

    @Override
    public void fetchAndSave() {
        var futures = SYMBOLS.stream()
                .map(pair -> executor.submit(() -> fetchSymbol(pair.symbol, pair.stdSymbol)))
                .toList();
        for (var f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                // Ignore
            }
        }
    }

    private void fetchSymbol(String symbol, String stdSymbol) {
        try {
            String fundingJson = restTemplate.getForObject(String.format(FUNDING_URL, symbol), String.class);
            if (fundingJson != null) {
                JsonNode root = objectMapper.readTree(fundingJson);
                if (root.path("code").asInt() == 0 && root.has("data") && root.get("data").isArray()) {
                    JsonNode arr = root.get("data");
                    if (arr.size() > 0) {
                        JsonNode first = arr.get(0);
                        BigDecimal rate = new BigDecimal(first.path("fundingRate").asText());
                        Long nextTime = first.path("fundingTime").asLong();
                        marketDataService.saveFundingRate("bingx", stdSymbol, rate, nextTime);
                    }
                }
            }

            String premiumJson = restTemplate.getForObject(String.format(PREMIUM_INDEX_URL, symbol), String.class);
            if (premiumJson != null) {
                JsonNode root = objectMapper.readTree(premiumJson);
                if (root.path("code").asInt() == 0 && root.has("data")) {
                    JsonNode data = root.get("data");
                    BigDecimal markPrice = parsePrice(data, "markPrice");
                    BigDecimal indexPrice = parsePrice(data, "indexPrice");
                    if (markPrice != null) marketDataService.saveFuturesPrice("bingx", stdSymbol, markPrice);
                    if (indexPrice != null) marketDataService.saveSpotPrice("bingx", stdSymbol, indexPrice);
                }
            }
            String spotJson = restTemplate.getForObject(String.format(SPOT_PRICE_URL, symbol), String.class);
            if (spotJson != null) {
                JsonNode root = objectMapper.readTree(spotJson);
                if (root.path("code").asInt() == 0 && root.has("data")) {
                    BigDecimal spotPrice = parsePrice(root.get("data"), "price");
                    if (spotPrice != null) marketDataService.saveSpotPrice("bingx", stdSymbol, spotPrice);
                }
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    private BigDecimal parsePrice(JsonNode node, String key) {
        if (node == null || !node.has(key)) return null;
        try {
            return new BigDecimal(node.path(key).asText());
        } catch (Exception e) {
            return null;
        }
    }

    private record SymbolPair(String symbol, String stdSymbol) {}
}
