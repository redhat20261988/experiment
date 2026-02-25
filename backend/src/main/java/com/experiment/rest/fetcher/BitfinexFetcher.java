package com.experiment.rest.fetcher;

import com.experiment.rest.HttpExchangeFetcher;
import com.experiment.service.MarketDataService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;

/**
 * Bitfinex HTTP Fetcher - 资金费率（WebSocket的FRR是P2P借贷利率，不是永续合约资金费率）。
 * 使用Derivatives Status API获取正确的8小时资金费率。
 * API: https://docs.bitfinex.com/reference/rest-public-derivatives-status
 */
public class BitfinexFetcher implements HttpExchangeFetcher {

    private static final String DERIVATIVES_STATUS_URL = "https://api-pub.bitfinex.com/v2/status/deriv?keys=tBTCF0:USTF0,tETHF0:USTF0,tSOLF0:USTF0,tXRPF0:USTF0,tDOGEF0:USTF0,tBNBF0:USTF0";

    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BitfinexFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "bitfinex";
    }

    @Override
    public void fetchAndSave() {
        try {
            String responseJson = restTemplate.getForObject(DERIVATIVES_STATUS_URL, String.class);
            if (responseJson != null) {
                JsonNode root = objectMapper.readTree(responseJson);
                if (root.isArray()) {
                    for (JsonNode item : root) {
                        if (item.isArray() && item.size() > 12) {
                            String key = item.get(0).asText();
                            BigDecimal currentFunding = parseDecimal(item, 12);
                            Long nextFundingTime = item.size() > 8 ? parseLong(item, 8) : null;
                            // [3] DERIV_PRICE 期货价格, [4] SPOT_PRICE 现货价格, [15] MARK_PRICE
                            BigDecimal derivPrice = parseDecimal(item, 3);
                            if (derivPrice == null) derivPrice = parseDecimal(item, 15);
                            BigDecimal spotPrice = parseDecimal(item, 4);

                            String stdSymbol = keyToStdSymbol(key);
                            if (stdSymbol != null) {
                                if (currentFunding != null) {
                                    marketDataService.saveFundingRate("bitfinex", stdSymbol, currentFunding, nextFundingTime);
                                }
                                if (derivPrice != null && derivPrice.compareTo(BigDecimal.ZERO) > 0) {
                                    marketDataService.saveFuturesPrice("bitfinex", stdSymbol, derivPrice);
                                }
                                if (spotPrice != null && spotPrice.compareTo(BigDecimal.ZERO) > 0) {
                                    marketDataService.saveSpotPrice("bitfinex", stdSymbol, spotPrice);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Ignore fetch errors
        }
    }

    private BigDecimal parseDecimal(JsonNode node, int index) {
        if (!node.isArray() || node.size() <= index) return null;
        JsonNode n = node.get(index);
        if (n.isNull()) return null;
        try {
            if (n.isNumber()) {
                return n.decimalValue();
            }
            String s = n.asText();
            if (s == null || s.isEmpty()) return null;
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String keyToStdSymbol(String key) {
        return switch (key) {
            case "tBTCF0:USTF0" -> "BTCUSDT";
            case "tETHF0:USTF0" -> "ETHUSDT";
            case "tSOLF0:USTF0" -> "SOLUSDT";
            case "tXRPF0:USTF0" -> "XRPUSDT";
            case "tDOGEF0:USTF0" -> "DOGEUSDT";
            case "tBNBF0:USTF0" -> "BNBUSDT";
            default -> null;
        };
    }

    private Long parseLong(JsonNode node, int index) {
        if (!node.isArray() || node.size() <= index) return null;
        JsonNode n = node.get(index);
        if (n.isNull()) return null;
        try {
            if (n.isNumber()) {
                return n.asLong();
            }
            String s = n.asText();
            if (s == null || s.isEmpty()) return null;
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
