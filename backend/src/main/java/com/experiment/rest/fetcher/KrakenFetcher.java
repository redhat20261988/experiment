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

/**
 * Kraken Futures HTTP Fetcher。
 * 调用 https://futures.kraken.com/derivatives/api/v3/tickers 接口获取资金费率。
 * 使用 PF_ 永续合约（如 PF_XBTUSD），资金费率 = fundingRate/markPrice，为每小时原值，接口返回 8 小时费率需乘以 8。
 * API: https://docs.futures.kraken.com/
 */
public class KrakenFetcher implements HttpExchangeFetcher {

    private static final String TICKER_URL = "https://futures.kraken.com/derivatives/api/v3/tickers";
    private static final BigDecimal EIGHT = new BigDecimal("8");

    // PF_=perpetual futures，资金费率 = fundingRate/markPrice（每小时），×8 为 8 小时费率
    private static final List<FundingSymbol> FUNDING_SYMBOLS = List.of(
            new FundingSymbol("PF_XBTUSD", "BTCUSDT"),
            new FundingSymbol("PF_ETHUSD", "ETHUSDT"),
            new FundingSymbol("PF_SOLUSD", "SOLUSDT"),
            new FundingSymbol("PF_XRPUSD", "XRPUSDT"),
            new FundingSymbol("PF_DOGEUSD", "DOGEUSDT"),
            new FundingSymbol("PF_BNBUSD", "BNBUSDT"));

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KrakenFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "kraken";
    }

    @Override
    public void fetchAndSave() {
        String tickerJson = fetchAllTickers();
        if (tickerJson == null) return;
        try {
            JsonNode tickers = parseTickersArray(tickerJson);
            if (tickers == null || !tickers.isArray()) return;
            for (FundingSymbol pair : FUNDING_SYMBOLS) {
                JsonNode ticker = findTickerBySymbol(tickers, pair.krakenSymbol);
                if (ticker != null && !parseBool(ticker, "suspended")) {
                    processTicker(ticker, pair.stdSymbol);
                }
            }
        } catch (Exception e) {
            log.warn("[kraken] fetchAndSave failed: {}", e.getMessage());
        }
    }

    private String fetchAllTickers() {
        try {
            return restTemplate.getForObject(TICKER_URL, String.class);
        } catch (Exception e) {
            log.warn("[kraken] fetch tickers failed: {}", e.getMessage());
            return null;
        }
    }

    private JsonNode parseTickersArray(String tickerJson) throws com.fasterxml.jackson.core.JsonProcessingException {
        JsonNode root = objectMapper.readTree(tickerJson);
        if (root.has("result") && "error".equals(root.get("result").asText())) return null;
        if (root.has("tickers") && root.get("tickers").isArray()) return root.get("tickers");
        return null;
    }

    private JsonNode findTickerBySymbol(JsonNode tickers, String symbol) {
        for (JsonNode t : tickers) {
            String sym = t.has("symbol") ? t.get("symbol").asText() : null;
            if (symbol.equals(sym)) return t;
        }
        return null;
    }

    /** 8h 资金费率 = (fundingRate/markPrice) * 8，其中 fundingRate/markPrice 为每小时原值 */
    private void processTicker(JsonNode ticker, String stdSymbol) {
        BigDecimal fundingRateRaw = parseDecimal(ticker, "fundingRate");
        BigDecimal markPrice = parseDecimal(ticker, "markPrice");
        if (markPrice == null) markPrice = parseDecimal(ticker, "mark_price");
        if (markPrice == null) markPrice = parseDecimal(ticker, "mark");
        if (fundingRateRaw != null && markPrice != null && markPrice.compareTo(BigDecimal.ZERO) != 0) {
            BigDecimal hourlyRate = fundingRateRaw.divide(markPrice, 12, java.math.RoundingMode.HALF_UP);
            BigDecimal rate8h = hourlyRate.multiply(EIGHT);
            marketDataService.saveFundingRate("kraken", stdSymbol, rate8h, null);
        }

        BigDecimal indexPrice = parseDecimal(ticker, "indexPrice");
        if (indexPrice == null) indexPrice = parseDecimal(ticker, "index_price");
        if (indexPrice == null) indexPrice = parseDecimal(ticker, "index");
        BigDecimal lastPrice = parseDecimal(ticker, "last");
        if (lastPrice == null) lastPrice = parseDecimal(ticker, "lastPrice");
        if (lastPrice == null) lastPrice = parseDecimal(ticker, "price");

        if (markPrice != null) marketDataService.saveFuturesPrice("kraken", stdSymbol, markPrice);
        if (indexPrice != null) marketDataService.saveSpotPrice("kraken", stdSymbol, indexPrice);
        else if (lastPrice != null) marketDataService.saveSpotPrice("kraken", stdSymbol, lastPrice);
    }

    private boolean parseBool(JsonNode node, String key) {
        if (!node.has(key)) return false;
        JsonNode n = node.get(key);
        if (n == null || n.isNull()) return false;
        return Boolean.TRUE.equals(n.asBoolean());
    }

    private record FundingSymbol(String krakenSymbol, String stdSymbol) {}

    private BigDecimal parseDecimal(JsonNode node, String key) {
        if (!node.has(key)) return null;
        JsonNode n = node.get(key);
        if (n == null || n.isNull()) return null;
        if (n.isNumber()) return n.decimalValue();
        String s = n.asText();
        if (s == null || s.isEmpty()) return null;
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

}
