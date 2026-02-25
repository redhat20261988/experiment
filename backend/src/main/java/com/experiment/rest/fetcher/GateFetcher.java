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
 * Gate.io HTTP Fetcher - 资金费率、期货价格、现货价格兜底（原仅 WebSocket，配合 2s Redis TTL 每秒刷新）。
 * API: https://fx-api.gateio.ws/api/v4/futures/usdt/tickers
 */
public class GateFetcher implements HttpExchangeFetcher {

    private static final String TICKERS_URL = "https://fx-api.gateio.ws/api/v4/futures/usdt/tickers";
    private static final List<String> CONTRACTS = List.of("BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "HYPE_USDT", "DOGE_USDT", "BNB_USDT");
    private static final List<String> STD_SYMBOLS = List.of("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "DOGEUSDT", "BNBUSDT");

    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public GateFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "gateio";
    }

    @Override
    public void fetchAndSave() {
        executor.submit(this::fetchAll);
    }

    private void fetchAll() {
        try {
            String json = restTemplate.getForObject(TICKERS_URL, String.class);
            if (json == null) return;
            JsonNode arr = objectMapper.readTree(json);
            if (!arr.isArray()) return;
            for (int i = 0; i < CONTRACTS.size(); i++) {
                String contract = CONTRACTS.get(i);
                String stdSymbol = STD_SYMBOLS.get(i);
                for (JsonNode item : arr) {
                    String c = item.has("contract") ? item.get("contract").asText() : "";
                    if (!contract.equals(c)) continue;
                    BigDecimal rate = parseDecimal(item, "funding_rate");
                    if (rate != null) marketDataService.saveFundingRate("gateio", stdSymbol, rate, null);
                    BigDecimal markPrice = parseDecimal(item, "mark_price");
                    BigDecimal last = parseDecimal(item, "last");
                    if (markPrice != null || last != null) {
                        marketDataService.saveFuturesPrice("gateio", stdSymbol, markPrice != null ? markPrice : last);
                    }
                    BigDecimal indexPrice = parseDecimal(item, "index_price");
                    if (indexPrice != null) marketDataService.saveSpotPrice("gateio", stdSymbol, indexPrice);
                    break;
                }
            }
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
