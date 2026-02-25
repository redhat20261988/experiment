package com.experiment.rest.fetcher;

import com.experiment.rest.HttpExchangeFetcher;
import com.experiment.service.MarketDataService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * dYdX HTTP Fetcher - 资金费率、期货价格、现货价格。
 * /v4/markets/{market} 返回 404，改用 perpetualMarkets（资金费率+现货 oraclePrice）+ orderbook mid（期货）。
 */
public class DydxFetcher implements HttpExchangeFetcher {

    private static final String PERPETUAL_MARKETS_URL = "https://indexer.dydx.trade/v4/perpetualMarkets";
    private static final String ORDERBOOK_URL = "https://indexer.dydx.trade/v4/orderbooks/perpetualMarket/%s";

    private static final String[] MARKET_KEYS = {"BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "BNB-USD"};
    private static final String[] SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT"};

    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public DydxFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "dydx";
    }

    @Override
    public void fetchAndSave() {
        try {
            String json = restTemplate.getForObject(PERPETUAL_MARKETS_URL, String.class);
            if (json == null) return;
            JsonNode root = objectMapper.readTree(json);
            if (!root.has("markets")) return;
            JsonNode markets = root.get("markets");

            var tasks = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < MARKET_KEYS.length; i++) {
                int idx = i;
                if (!markets.has(MARKET_KEYS[idx])) continue;
                JsonNode m = markets.get(MARKET_KEYS[idx]);
                if (!"ACTIVE".equals(m.path("status").asText(null))) continue;

                BigDecimal rate = parseDecimal(m, "nextFundingRate");
                if (rate != null) marketDataService.saveFundingRate("dydx", SYMBOLS[idx], rate, null);

                BigDecimal oraclePrice = parseDecimal(m, "oraclePrice");
                if (oraclePrice != null) marketDataService.saveSpotPrice("dydx", SYMBOLS[idx], oraclePrice);

                tasks.add(executor.submit(() -> fetchOrderbookMid(SYMBOLS[idx], MARKET_KEYS[idx])));
            }
            for (var f : tasks) {
                try { f.get(); } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            // Ignore fetch errors
        }
    }

    private void fetchOrderbookMid(String symbol, String marketKey) {
        try {
            String obJson = restTemplate.getForObject(String.format(ORDERBOOK_URL, marketKey), String.class);
            if (obJson == null) return;
            JsonNode ob = objectMapper.readTree(obJson);
            JsonNode bids = ob.path("bids");
            JsonNode asks = ob.path("asks");
            if (!bids.isArray() || bids.size() == 0 || !asks.isArray() || asks.size() == 0) return;
            BigDecimal bestBid = parseDecimal(bids.get(0), "price");
            BigDecimal bestAsk = parseDecimal(asks.get(0), "price");
            if (bestBid != null && bestAsk != null) {
                BigDecimal mid = bestBid.add(bestAsk).divide(BigDecimal.valueOf(2), 8, RoundingMode.HALF_UP);
                marketDataService.saveFuturesPrice("dydx", symbol, mid);
            }
        } catch (Exception e) {
            // Ignore
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
}
