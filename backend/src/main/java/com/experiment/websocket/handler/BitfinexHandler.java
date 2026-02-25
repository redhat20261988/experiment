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
 * Bitfinex WebSocket - 永续期货资金费率、期货价格、现货价格。
 * WebSocket: wss://api-pub.bitfinex.com/ws/2
 */
public class BitfinexHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://api-pub.bitfinex.com/ws/2";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BitfinexHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("bitfinex", URI.create(WS_URL), this);
    }

    private java.util.Map<Integer, String> channelIdToSymbol = new java.util.HashMap<>();

    @Override
    public void onConnected(ManagedWebSocket client) {
        log.info("Bitfinex WebSocket connected");
        try {
            // Subscribe to ticker for BTCUSD and ETHUSD (spot)
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"tBTCUSD\"}");
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"tETHUSD\"}");
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"tSOLUSD\"}");
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"tXRPUSD\"}");
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"tDOGEUSD\"}");
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"tBNBUSD\"}");
            // Subscribe to perpetual ticker for BTCUSD and ETHUSD (fUSD for funding rate, tBTCUSD:USD and tETHUSD:USD for prices)
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"tBTCUSD:USD\"}");
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"tETHUSD:USD\"}");
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"tSOLUSD:USD\"}");
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"tXRPUSD:USD\"}");
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"tDOGEUSD:USD\"}");
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"tBNBUSD:USD\"}");
            // Subscribe to funding ticker for perpetuals (fUSD contains FRR - Flash Return Rate)
            client.send("{\"event\":\"subscribe\",\"channel\":\"ticker\",\"symbol\":\"fUSD\"}");
        } catch (Exception e) {
            log.warn("Bitfinex subscribe error: {}", e.getMessage());
        }
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            
            // Handle subscription confirmation
            if (root.has("event") && "subscribed".equals(root.get("event").asText())) {
                int channelId = root.has("chanId") ? root.get("chanId").asInt() : -1;
                String symbol = root.has("symbol") ? root.get("symbol").asText() : "";
                if (channelId >= 0 && !symbol.isEmpty()) {
                    channelIdToSymbol.put(channelId, symbol);
                }
                return;
            }
            
            // Bitfinex sends array format: [channelId, [data...]]
            if (root.isArray() && root.size() >= 2) {
                int channelId = root.get(0).asInt();
                JsonNode data = root.get(1);
                
                String symbolKey = channelIdToSymbol.get(channelId);
                
                if (data.isArray() && data.size() >= 10) {
                    if ("fUSD".equals(symbolKey)) {
                        // FRR (Flash Return Rate) is P2P lending rate, not perpetual funding rate.
                        // Perpetual funding rate is now fetched via HTTP API (BitfinexFetcher) from Derivatives Status API.
                        // Skip FRR - it's not the correct funding rate for perpetuals.
                    } else if (symbolKey != null && symbolKey.startsWith("t")) {
                        // Spot ticker format: [BID, BID_SIZE, ASK, ASK_SIZE, DAILY_CHANGE, DAILY_CHANGE_PERC, LAST_PRICE, VOLUME, HIGH, LOW]
                        BigDecimal lastPrice = parseDecimal(data, 6); // LAST_PRICE at index 6
                        String symbol = null;
                        if ("tBTCUSD".equals(symbolKey)) {
                            symbol = "BTCUSDT";
                            if (lastPrice != null && lastPrice.compareTo(BigDecimal.ZERO) > 0) {
                                // Bitfinex现货ticker只提供现货价格
                                marketDataService.saveSpotPrice("bitfinex", symbol, lastPrice);
                            }
                        } else if ("tETHUSD".equals(symbolKey)) {
                            symbol = "ETHUSDT";
                            if (lastPrice != null && lastPrice.compareTo(BigDecimal.ZERO) > 0) {
                                // Bitfinex现货ticker只提供现货价格
                                marketDataService.saveSpotPrice("bitfinex", symbol, lastPrice);
                            }
                        } else if ("tSOLUSD".equals(symbolKey)) {
                            symbol = "SOLUSDT";
                            if (lastPrice != null && lastPrice.compareTo(BigDecimal.ZERO) > 0) {
                                marketDataService.saveSpotPrice("bitfinex", symbol, lastPrice);
                            }
                        } else if ("tXRPUSD".equals(symbolKey)) {
                            symbol = "XRPUSDT";
                            if (lastPrice != null && lastPrice.compareTo(BigDecimal.ZERO) > 0) {
                                marketDataService.saveSpotPrice("bitfinex", symbol, lastPrice);
                            }
                        } else if ("tDOGEUSD".equals(symbolKey)) {
                            symbol = "DOGEUSDT";
                            if (lastPrice != null && lastPrice.compareTo(BigDecimal.ZERO) > 0) {
                                marketDataService.saveSpotPrice("bitfinex", symbol, lastPrice);
                            }
                        } else if ("tBNBUSD".equals(symbolKey)) {
                            symbol = "BNBUSDT";
                            if (lastPrice != null && lastPrice.compareTo(BigDecimal.ZERO) > 0) {
                                marketDataService.saveSpotPrice("bitfinex", symbol, lastPrice);
                            }
                        } else if ("tBTCUSD:USD".equals(symbolKey)) {
                            // Perpetual ticker for BTC - format same as spot ticker
                            symbol = "BTCUSDT";
                            if (lastPrice != null && lastPrice.compareTo(BigDecimal.ZERO) > 0) {
                                marketDataService.saveFuturesPrice("bitfinex", symbol, lastPrice);
                            }
                        } else if ("tETHUSD:USD".equals(symbolKey)) {
                            // Perpetual ticker for ETH - format same as spot ticker
                            symbol = "ETHUSDT";
                            if (lastPrice != null && lastPrice.compareTo(BigDecimal.ZERO) > 0) {
                                marketDataService.saveFuturesPrice("bitfinex", symbol, lastPrice);
                            }
                        } else if ("tSOLUSD:USD".equals(symbolKey)) {
                            symbol = "SOLUSDT";
                            if (lastPrice != null && lastPrice.compareTo(BigDecimal.ZERO) > 0) {
                                marketDataService.saveFuturesPrice("bitfinex", symbol, lastPrice);
                            }
                        } else if ("tXRPUSD:USD".equals(symbolKey)) {
                            symbol = "XRPUSDT";
                            if (lastPrice != null && lastPrice.compareTo(BigDecimal.ZERO) > 0) {
                                marketDataService.saveFuturesPrice("bitfinex", symbol, lastPrice);
                            }
                        } else if ("tDOGEUSD:USD".equals(symbolKey)) {
                            symbol = "DOGEUSDT";
                            if (lastPrice != null && lastPrice.compareTo(BigDecimal.ZERO) > 0) {
                                marketDataService.saveFuturesPrice("bitfinex", symbol, lastPrice);
                            }
                        } else if ("tBNBUSD:USD".equals(symbolKey)) {
                            symbol = "BNBUSDT";
                            if (lastPrice != null && lastPrice.compareTo(BigDecimal.ZERO) > 0) {
                                marketDataService.saveFuturesPrice("bitfinex", symbol, lastPrice);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("Bitfinex parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("Bitfinex parse error: {}", e.getMessage());
            }
        }
    }

    private BigDecimal parseDecimal(JsonNode node, int index) {
        if (!node.isArray() || node.size() <= index) return null;
        JsonNode n = node.get(index);
        if (n.isNull()) return null;
        try {
            if (n.isNumber()) {
                return new BigDecimal(n.asDouble());
            }
            return new BigDecimal(n.asText());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
