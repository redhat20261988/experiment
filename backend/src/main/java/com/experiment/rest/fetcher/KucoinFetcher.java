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
 * Kucoin 纯 HTTP。6 交易对 × 3 请求串行易超 2s TTL，改为并发拉取。
 */
public class KucoinFetcher implements HttpExchangeFetcher {

    private static final String FUNDING_URL = "https://api.kucoin.com/api/ua/v1/market/funding-rate?symbol=%s";
    private static final String TICKER_URL = "https://api-futures.kucoin.com/api/v1/ticker?symbol=%s";
    private static final String SPOT_TICKER_URL = "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=%s";

    /** 期货 symbol -> 现货 symbol（Kucoin 现货用 BTC-USDT，期货用 XBTUSDTM） */
    private static final List<SymbolPair> SYMBOLS = List.of(
            new SymbolPair("XBTUSDTM", "BTCUSDT", "BTC-USDT"), new SymbolPair("ETHUSDTM", "ETHUSDT", "ETH-USDT"),
            new SymbolPair("SOLUSDTM", "SOLUSDT", "SOL-USDT"), new SymbolPair("XRPUSDTM", "XRPUSDT", "XRP-USDT"),
            new SymbolPair("DOGEUSDTM", "DOGEUSDT", "DOGE-USDT"), new SymbolPair("BNBUSDTM", "BNBUSDT", "BNB-USDT"));

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public KucoinFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "kucoin";
    }

    @Override
    public void fetchAndSave() {
        var futures = SYMBOLS.stream()
                .map(p -> executor.submit(() -> fetchSymbol(p.kucoinSymbol, p.stdSymbol, p.spotSymbol)))
                .toList();
        for (var f : futures) {
            try { f.get(); } catch (Exception ignored) {}
        }
    }

    private void fetchSymbol(String kucoinSymbol, String stdSymbol, String spotSymbol) {
        try {
            String fundingJson = restTemplate.getForObject(String.format(FUNDING_URL, kucoinSymbol), String.class);
            if (fundingJson != null) {
                JsonNode root = objectMapper.readTree(fundingJson);
                if ("200000".equals(root.path("code").asText())) {
                    JsonNode data = root.get("data");
                    BigDecimal rate = new BigDecimal(data.path("nextFundingRate").asText());
                    Long nextTime = data.path("fundingTime").asLong();
                    marketDataService.saveFundingRate("kucoin", stdSymbol, rate, nextTime);
                }
            }

            String tickerJson = restTemplate.getForObject(String.format(TICKER_URL, kucoinSymbol), String.class);
            if (tickerJson != null) {
                JsonNode root = objectMapper.readTree(tickerJson);
                if ("200000".equals(root.path("code").asText())) {
                    JsonNode data = root.get("data");
                    BigDecimal price = new BigDecimal(data.path("price").asText());
                    // Kucoin futures ticker API只提供期货价格
                    marketDataService.saveFuturesPrice("kucoin", stdSymbol, price);
                }
            }
            
            // 获取现货价格（orderbook level1：price 可能为空，用 bestBid/bestAsk 中点兜底。现货用 BTC-USDT，期货用 XBTUSDTM）
            String spotTickerJson = restTemplate.getForObject(String.format(SPOT_TICKER_URL, spotSymbol), String.class);
            if (spotTickerJson != null) {
                JsonNode root = objectMapper.readTree(spotTickerJson);
                if ("200000".equals(root.path("code").asText())) {
                    JsonNode data = root.get("data");
                    BigDecimal spotPrice = parseDecimal(data, "price");
                    if (spotPrice == null) {
                        BigDecimal bid = parseDecimal(data, "bestBid");
                        BigDecimal ask = parseDecimal(data, "bestAsk");
                        if (bid != null && ask != null) {
                            spotPrice = bid.add(ask).divide(BigDecimal.valueOf(2));
                        }
                    }
                    if (spotPrice != null) {
                        marketDataService.saveSpotPrice("kucoin", stdSymbol, spotPrice);
                    }
                }
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

    private record SymbolPair(String kucoinSymbol, String stdSymbol, String spotSymbol) {}
}
