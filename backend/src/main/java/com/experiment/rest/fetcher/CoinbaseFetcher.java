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
 * Coinbase INTX 资金费率 REST（需 API 认证）。
 * 当前未配置认证，仅尝试拉取；若 401 则静默跳过。
 */
public class CoinbaseFetcher implements HttpExchangeFetcher {

    private static final String FUNDING_URL = "https://api.international.coinbase.com/api/v1/instruments/%s/funding?result_limit=1";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CoinbaseFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "coinbase";
    }

    @Override
    public void fetchAndSave() {
        fetchFunding("BTC-PERP", "BTCUSDT");
        fetchFunding("ETH-PERP", "ETHUSDT");
        fetchFunding("SOL-PERP", "SOLUSDT");
        fetchFunding("XRP-PERP", "XRPUSDT");
        fetchFunding("DOGE-PERP", "DOGEUSDT");
        fetchFunding("BNB-PERP", "BNBUSDT");
    }

    private void fetchFunding(String instrument, String stdSymbol) {
        try {
            String json = restTemplate.getForObject(String.format(FUNDING_URL, instrument), String.class);
            if (json == null) return;
            JsonNode root = objectMapper.readTree(json);
            JsonNode arr = root.isArray() ? root : root.has("funding_rates") ? root.get("funding_rates") : null;
            if (arr != null && arr.isArray() && arr.size() > 0) {
                JsonNode first = arr.get(0);
                BigDecimal rate = new BigDecimal(first.path("funding_rate").asText());
                String eventTime = first.path("event_time").asText();
                long nextTime = java.time.Instant.parse(eventTime).toEpochMilli() + 8 * 3600 * 1000;
                marketDataService.saveFundingRate("coinbase", stdSymbol, rate, nextTime);
            }
        } catch (org.springframework.web.client.HttpClientErrorException.Unauthorized e) {
            // INTX 需认证，静默跳过
        } catch (Exception e) {
            // Ignore fetch errors
        }
    }
}
