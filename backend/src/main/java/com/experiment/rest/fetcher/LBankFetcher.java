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
 * LBank HTTP Fetcher - 资金费率、期货价格、现货价格。
 * API: https://www.lbank.com/docs/
 */
public class LBankFetcher implements HttpExchangeFetcher {

    /** 使用 api.lbkex.com 及官方推荐的 ticker/24hr.do；价格在 data[].ticker.latest */
    private static final String TICKER_URL = "https://api.lbkex.com/v2/ticker/24hr.do?symbol=%s";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public LBankFetcher(MarketDataService marketDataService, RestTemplate restTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
    }

    @Override
    public String getExchangeName() {
        return "lbank";
    }

    @Override
    public void fetchAndSave() {
        fetchSymbol("btc_usdt", "BTCUSDT");
        fetchSymbol("eth_usdt", "ETHUSDT");
        fetchSymbol("sol_usdt", "SOLUSDT");
        fetchSymbol("xrp_usdt", "XRPUSDT");
        fetchSymbol("doge_usdt", "DOGEUSDT");
        fetchSymbol("bnb_usdt", "BNBUSDT");
        fetchFundingRate();
    }
    
    private void fetchFundingRate() {
        try {
            String url = "https://lbkperp.lbank.com/cfd/openApi/v1/pub/marketData?productGroup=SwapU";
            String marketDataJson = restTemplate.getForObject(url, String.class);
            if (marketDataJson != null) {
                JsonNode root = objectMapper.readTree(marketDataJson);
                // LBank may return {"code": 0, "data": [...]} or direct array
                JsonNode data = root.has("data") ? root.get("data") : root;
                if (data.isArray()) {
                    for (JsonNode item : data) {
                        String symbol = item.has("symbol") ? item.get("symbol").asText() : null;
                        if (symbol != null) {
                            // 精确匹配主合约，避免 PUMPBTCUSDT/BTCDOMUSDT 等被误当作 BTCUSDT 导致资金费率跳变
                            String symbolNorm = symbol.toUpperCase().replace("-", "").replace("_", "");
                            String stdSymbol = null;
                            if ("BTCUSDT".equals(symbolNorm)) stdSymbol = "BTCUSDT";
                            else if ("ETHUSDT".equals(symbolNorm)) stdSymbol = "ETHUSDT";
                            else if ("SOLUSDT".equals(symbolNorm)) stdSymbol = "SOLUSDT";
                            else if ("XRPUSDT".equals(symbolNorm)) stdSymbol = "XRPUSDT";
                            else if ("DOGEUSDT".equals(symbolNorm)) stdSymbol = "DOGEUSDT";
                            else if ("BNBUSDT".equals(symbolNorm)) stdSymbol = "BNBUSDT";
                            
                            if (stdSymbol != null) {
                                // Try different field names for funding rate
                                BigDecimal fundingRate = parseDecimal(item, "fundingRate");
                                if (fundingRate == null) {
                                    fundingRate = parseDecimal(item, "funding_rate");
                                }
                                if (fundingRate == null) {
                                    fundingRate = parseDecimal(item, "prePositionFeeRate");
                                }
                                if (fundingRate == null) {
                                    fundingRate = parseDecimal(item, "pre_position_fee_rate");
                                }
                                
                                Long nextFundingTime = null;
                                if (item.has("nextFundingTime")) {
                                    try {
                                        nextFundingTime = Long.parseLong(item.get("nextFundingTime").asText());
                                    } catch (NumberFormatException e) {
                                        // ignore
                                    }
                                } else if (item.has("nextFeeTime")) {
                                    try {
                                        nextFundingTime = item.get("nextFeeTime").asLong();
                                    } catch (Exception e) {
                                        // ignore
                                    }
                                } else if (item.has("next_funding_time")) {
                                    try {
                                        nextFundingTime = Long.parseLong(item.get("next_funding_time").asText());
                                    } catch (NumberFormatException e) {
                                        // ignore
                                    }
                                }
                                
                                if (fundingRate != null) {
                                    marketDataService.saveFundingRate("lbank", stdSymbol, fundingRate, nextFundingTime);
                                }
                                
                                // Also update futures price from marketData if available
                                BigDecimal markedPrice = parseDecimal(item, "markedPrice");
                                if (markedPrice == null) {
                                    markedPrice = parseDecimal(item, "marked_price");
                                }
                                if (markedPrice == null) {
                                    markedPrice = parseDecimal(item, "markPrice");
                                }
                                BigDecimal lastPrice = parseDecimal(item, "lastPrice");
                                if (lastPrice == null) {
                                    lastPrice = parseDecimal(item, "last_price");
                                }
                                
                                if (markedPrice != null) {
                                    marketDataService.saveFuturesPrice("lbank", stdSymbol, markedPrice);
                                } else if (lastPrice != null) {
                                    marketDataService.saveFuturesPrice("lbank", stdSymbol, lastPrice);
                                }
                                
                                // 保存现货价格（indexPrice / underlyingPrice）
                                // 不应fallback到期货价格（lastPrice），因为会导致价差为0
                                BigDecimal indexPrice = parseDecimal(item, "indexPrice");
                                if (indexPrice == null) {
                                    indexPrice = parseDecimal(item, "index_price");
                                }
                                if (indexPrice == null) {
                                    indexPrice = parseDecimal(item, "underlyingPrice");
                                }
                                if (indexPrice != null) {
                                    marketDataService.saveSpotPrice("lbank", stdSymbol, indexPrice);
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

    private void fetchSymbol(String lbankSymbol, String stdSymbol) {
        try {
            String url = String.format(TICKER_URL, lbankSymbol);
            String tickerJson = restTemplate.getForObject(url, String.class);
            if (tickerJson != null) {
                JsonNode root = objectMapper.readTree(tickerJson);
                // LBank API可能返回 {"data": [...]} 或 {"data": {...}} 格式
                JsonNode dataNode = root.has("data") ? root.get("data") : root;
                
                JsonNode data = null;
                if (dataNode.isArray() && dataNode.size() > 0) {
                    data = dataNode.get(0);
                } else if (!dataNode.isArray()) {
                    data = dataNode;
                }
                
                if (data != null) {
                    // LBank ticker/24hr 结构：data[0] = {symbol, ticker: {latest, high, low, ...}, timestamp}
                    BigDecimal lastPrice = null;
                    if (data.has("ticker")) {
                        lastPrice = parseDecimal(data.get("ticker"), "latest");
                    }
                    if (lastPrice == null) {
                        lastPrice = parseDecimal(data, "latest");
                    }
                    if (lastPrice == null) {
                        lastPrice = parseDecimal(data, "last");
                    }
                    if (lastPrice == null) {
                        lastPrice = parseDecimal(data, "close");
                    }
                    if (lastPrice == null) {
                        lastPrice = parseDecimal(data, "price");
                    }
                    
                    if (lastPrice != null) {
                        // 校验返回的 symbol 与请求一致，避免 symbol=all 时取错元素
                        String respSymbol = data.has("symbol") ? data.get("symbol").asText().toLowerCase().replace("-", "_") : "";
                        if (respSymbol.isEmpty() || respSymbol.equals(lbankSymbol.toLowerCase())) {
                            marketDataService.saveSpotPrice("lbank", stdSymbol, lastPrice);
                        }
                        // 如果没有期货价格，也使用现货价格作为期货价格（作为fallback）
                        // 注意：fetchFundingRate方法会更新期货价格
                    }
                }
            }
        } catch (Exception e) {
            // Ignore fetch errors
        }
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
}
