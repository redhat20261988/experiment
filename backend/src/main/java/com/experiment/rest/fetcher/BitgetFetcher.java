package com.experiment.rest.fetcher;

import com.experiment.rest.HttpExchangeFetcher;
import com.experiment.service.MarketDataService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;

/**
 * Bitget HTTP Fetcher - 现货价格（WebSocket提供期货价格，现货价格需HTTP API）。
 * API V1 已下线，使用 V2: https://www.bitget.com/api-doc/common/release-note
 */
public class BitgetFetcher implements HttpExchangeFetcher {

    private static final String SPOT_TICKER_URL = "https://api.bitget.com/api/v2/spot/market/tickers?symbol=%s";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BitgetFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "bitget";
    }

    @Override
    public void fetchAndSave() {
        fetchSpotPrice("BTCUSDT", "BTCUSDT");
        fetchSpotPrice("ETHUSDT", "ETHUSDT");
        fetchSpotPrice("SOLUSDT", "SOLUSDT");
        fetchSpotPrice("XRPUSDT", "XRPUSDT");
        fetchSpotPrice("HYPEUSDT", "HYPEUSDT");
        fetchSpotPrice("DOGEUSDT", "DOGEUSDT");
        fetchSpotPrice("BNBUSDT", "BNBUSDT");
    }
    
    private void fetchSpotPrice(String bitgetSymbol, String stdSymbol) {
        try {
            String url = String.format(SPOT_TICKER_URL, bitgetSymbol);
            String tickerJson = restTemplate.getForObject(url, String.class);
            if (tickerJson != null) {
                JsonNode root = objectMapper.readTree(tickerJson);
                if (root.has("code") && "00000".equals(root.get("code").asText())) {
                    JsonNode data = root.get("data");
                    if (data != null && data.isArray() && data.size() > 0) {
                        JsonNode ticker = data.get(0);
                        BigDecimal spotPrice = parseDecimal(ticker, "lastPr");
                        if (spotPrice == null) spotPrice = parseDecimal(ticker, "bidPr");
                        if (spotPrice == null) spotPrice = parseDecimal(ticker, "askPr");
                        if (spotPrice != null) {
                            marketDataService.saveSpotPrice("bitget", stdSymbol, spotPrice);
                        }
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
}
