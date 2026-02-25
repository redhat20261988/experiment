package com.experiment.websocket.handler;

import com.experiment.service.MarketDataService;
import com.experiment.util.RedisShutdownUtil;
import com.experiment.websocket.ExchangeWebSocketHandler;
import com.experiment.websocket.ManagedWebSocket;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.net.URI;

/**
 * dYdX WebSocket - 永续期货资金费率、期货价格、现货价格。
 * WebSocket: wss://indexer.dydx.trade/v4/ws
 */
public class DydxHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://indexer.dydx.trade/v4/ws";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DydxHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("dydx", URI.create(WS_URL), this);
    }

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("dYdX WebSocket connected");
        try {
            // v4_markets: 资金费率+现货(oraclePrice/indexPrice); v4_trades: 期货(price)
            String subscribeMarkets = "{\"type\":\"subscribe\",\"channel\":\"v4_markets\",\"id\":\"BTC-USD\"}";
            client.send(subscribeMarkets);
            String subscribeMarketsEth = "{\"type\":\"subscribe\",\"channel\":\"v4_markets\",\"id\":\"ETH-USD\"}";
            client.send(subscribeMarketsEth);
            String subscribeMarketsSol = "{\"type\":\"subscribe\",\"channel\":\"v4_markets\",\"id\":\"SOL-USD\"}";
            client.send(subscribeMarketsSol);
            String subscribeMarketsXrp = "{\"type\":\"subscribe\",\"channel\":\"v4_markets\",\"id\":\"XRP-USD\"}";
            client.send(subscribeMarketsXrp);
            String subscribeMarketsDoge = "{\"type\":\"subscribe\",\"channel\":\"v4_markets\",\"id\":\"DOGE-USD\"}";
            client.send(subscribeMarketsDoge);
            String subscribeMarketsBnb = "{\"type\":\"subscribe\",\"channel\":\"v4_markets\",\"id\":\"BNB-USD\"}";
            client.send(subscribeMarketsBnb);
            for (String m : new String[]{"BTC-USD","ETH-USD","SOL-USD","XRP-USD","DOGE-USD","BNB-USD"}) {
                client.send("{\"type\":\"subscribe\",\"channel\":\"v4_trades\",\"id\":\""+m+"\"}");
            }
        } catch (Exception e) {
            log.warn("dYdX subscribe error: {}", e.getMessage());
        }
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            
            if (root.has("channel") && root.has("contents")) {
                String channel = root.get("channel").asText();
                String id = root.has("id") ? root.get("id").asText() : "";
                JsonNode contents = root.get("contents");
                
                if ("v4_markets".equals(channel)) {
                    // dYdX markets format: contents.markets.{BTC-USD: {...}}
                    if (contents.has("markets")) {
                        JsonNode markets = contents.get("markets");
                        // Process BTC-USD and ETH-USD markets
                        for (String marketKey : new String[]{"BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "BNB-USD"}) {
                            if (markets.has(marketKey)) {
                                JsonNode market = markets.get(marketKey);
                                String symbol = null;
                                if (marketKey.contains("BTC")) symbol = "BTCUSDT";
                                else if (marketKey.contains("ETH")) symbol = "ETHUSDT";
                                else if (marketKey.contains("SOL")) symbol = "SOLUSDT";
                                else if (marketKey.contains("XRP")) symbol = "XRPUSDT";
                                else if (marketKey.contains("DOGE")) symbol = "DOGEUSDT";
                                else if (marketKey.contains("BNB")) symbol = "BNBUSDT";
                                if (symbol == null) continue;
                                
                                // Parse funding rate - prefer currentFundingRate (actual current rate) over nextFundingRate (predicted rate)
                                BigDecimal rate = parseDecimal(market, "currentFundingRate");
                                if (rate == null) {
                                    // Fallback to nextFundingRate if currentFundingRate is not available
                                    rate = parseDecimal(market, "nextFundingRate");
                                }
                                if (rate != null) {
                                    marketDataService.saveFundingRate("dydx", symbol, rate, null);
                                }
                                
                                // lastPrice/indexPrice 可能不存在，用 oraclePrice 作为现货，期货由 v4_trades 提供
                                BigDecimal lastPrice = parseDecimal(market, "lastPrice");
                                BigDecimal indexPrice = parseDecimal(market, "indexPrice");
                                BigDecimal oraclePrice = parseDecimal(market, "oraclePrice");
                                if (lastPrice != null) marketDataService.saveFuturesPrice("dydx", symbol, lastPrice);
                                if (indexPrice != null) marketDataService.saveSpotPrice("dydx", symbol, indexPrice);
                                else if (oraclePrice != null) marketDataService.saveSpotPrice("dydx", symbol, oraclePrice);
                            }
                        }
                    }
                } else if ("v4_trades".equals(channel)) {
                    String symbol = null;
                    if (id.contains("BTC")) symbol = "BTCUSDT";
                    else if (id.contains("ETH")) symbol = "ETHUSDT";
                    else if (id.contains("SOL")) symbol = "SOLUSDT";
                    else if (id.contains("XRP")) symbol = "XRPUSDT";
                    else if (id.contains("DOGE")) symbol = "DOGEUSDT";
                    else if (id.contains("BNB")) symbol = "BNBUSDT";
                    if (symbol != null && contents.has("trades") && contents.get("trades").isArray()) {
                        JsonNode trades = contents.get("trades");
                        if (trades.size() > 0) {
                            BigDecimal price = parseDecimal(trades.get(trades.size() - 1), "price");
                            if (price != null) marketDataService.saveFuturesPrice("dydx", symbol, price);
                        }
                    }
                }
            } else {
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("dYdX parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("dYdX parse error: {}", e.getMessage());
            }
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
