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
 * Crypto.com HTTP Fetcher - 现货价格 + 资金费率兜底（WebSocket 推送间隔长、Redis 2s TTL，用 REST 每秒刷新）。
 * Spot: public/get-tickers?instrument_name=BTC_USDT
 * Funding: public/get-valuations?instrument_name=BTCUSD-PERP&valuation_type=funding_hist&count=1
 */
public class CryptoComFetcher implements HttpExchangeFetcher {

    private static final String SPOT_TICKER_URL = "https://api.crypto.com/exchange/v1/public/get-tickers?instrument_name=%s";
    private static final String FUNDING_URL = "https://api.crypto.com/exchange/v1/public/get-valuations?instrument_name=%s&valuation_type=funding_hist&count=1";
    private static final List<SymbolPair> SPOT_SYMBOLS = List.of(
            new SymbolPair("BTC_USDT", "BTCUSDT"), new SymbolPair("ETH_USDT", "ETHUSDT"),
            new SymbolPair("SOL_USDT", "SOLUSDT"), new SymbolPair("XRP_USDT", "XRPUSDT"),
            new SymbolPair("DOGE_USDT", "DOGEUSDT"), new SymbolPair("BNB_USDT", "BNBUSDT"));
    private static final List<FundingPair> FUNDING_SYMBOLS = List.of(
            new FundingPair("BTCUSD-PERP", "BTCUSDT"), new FundingPair("ETHUSD-PERP", "ETHUSDT"),
            new FundingPair("SOLUSD-PERP", "SOLUSDT"), new FundingPair("XRPUSD-PERP", "XRPUSDT"),
            new FundingPair("HYPEUSD-PERP", "HYPEUSDT"), new FundingPair("DOGEUSD-PERP", "DOGEUSDT"));

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public CryptoComFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "cryptocom";
    }

    @Override
    public void fetchAndSave() {
        for (SymbolPair p : SPOT_SYMBOLS) {
            executor.submit(() -> fetchSpotPrice(p.instrumentName, p.stdSymbol));
        }
        for (FundingPair p : FUNDING_SYMBOLS) {
            executor.submit(() -> fetchFundingRate(p.instrumentName, p.stdSymbol));
        }
    }

    private void fetchSpotPrice(String instrumentName, String stdSymbol) {
        try {
            String url = String.format(SPOT_TICKER_URL, instrumentName);
            String json = restTemplate.getForObject(url, String.class);
            if (json == null) return;
            JsonNode root = objectMapper.readTree(json);
            JsonNode result = root.path("result");
            JsonNode data = result.path("data");
            if (!data.isArray() || data.size() == 0) return;
            JsonNode ticker = data.get(0);
            // a = 最新成交价, b = best bid, k = best ask
            BigDecimal spotPrice = parseDecimal(ticker, "a");
            if (spotPrice == null) {
                BigDecimal bid = parseDecimal(ticker, "b");
                BigDecimal ask = parseDecimal(ticker, "k");
                if (bid != null && ask != null) {
                    spotPrice = bid.add(ask).divide(BigDecimal.valueOf(2));
                }
            }
            if (spotPrice != null) {
                marketDataService.saveSpotPrice("cryptocom", stdSymbol, spotPrice);
            }
        } catch (Exception e) {
            // Ignore fetch errors
        }
    }

    private BigDecimal parseDecimal(JsonNode node, String key) {
        if (node == null || !node.has(key)) return null;
        JsonNode n = node.get(key);
        if (n.isNull()) return null;
        try {
            if (n.isTextual()) {
                String s = n.asText();
                if (s != null && !s.isEmpty()) return new BigDecimal(s);
            } else if (n.isNumber()) {
                return n.decimalValue();
            }
        } catch (NumberFormatException e) {
            // ignore
        }
        return null;
    }

    private void fetchFundingRate(String instrumentName, String stdSymbol) {
        try {
            String url = String.format(FUNDING_URL, instrumentName);
            String json = restTemplate.getForObject(url, String.class);
            if (json == null) return;
            JsonNode root = objectMapper.readTree(json);
            if (root.path("code").asInt() != 0) return;
            JsonNode result = root.path("result");
            JsonNode data = result.path("data");
            if (!data.isArray() || data.size() == 0) return;
            JsonNode first = data.get(0);
            BigDecimal rate = parseDecimal(first, "v");
            if (rate != null) {
                marketDataService.saveFundingRate("cryptocom", stdSymbol, rate, null);
            }
        } catch (Exception e) {
            // Ignore fetch errors
        }
    }

    private record SymbolPair(String instrumentName, String stdSymbol) {}
    private record FundingPair(String instrumentName, String stdSymbol) {}
}
