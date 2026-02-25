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
 * Crypto.com Exchange WebSocket - 永续期货 funding、ticker、mark。
 * instrument 命名: BTCUSD-PERP, ETHUSD-PERP
 */
public class CryptoComHandler implements ExchangeWebSocketHandler {

    private static final String WS_URL = "wss://stream.crypto.com/exchange/v1/market";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MarketDataService marketDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private ManagedWebSocket client; // 存储client引用以便响应心跳

    public CryptoComHandler(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public ManagedWebSocket createClient() {
        return new ManagedWebSocket("cryptocom", URI.create(WS_URL), this);
    }

    @Override
    public void onConnected(ManagedWebSocket client) {
        this.client = client; // 保存client引用以便响应心跳
        log.info("Crypto.com WebSocket connected");
        // 根据API文档，建议连接后等待1秒再发送请求，避免rate limit错误
        // 使用异步方式延迟发送，避免阻塞
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                if (client != null && client.isOpen()) {
                    long nonce = System.currentTimeMillis();
                    // 修复：使用 "subscribe" 而不是 "public/subscribe"
                    // Crypto.com 不支持 BNBUSD-PERP (Unknown symbol)、index 通道 (Unsupported instrument)
                    String subscribe = """
                            {"id":1,"method":"subscribe","params":{"channels":["funding.BTCUSD-PERP","funding.ETHUSD-PERP","funding.SOLUSD-PERP","funding.XRPUSD-PERP","funding.HYPEUSD-PERP","funding.DOGEUSD-PERP","ticker.BTCUSD-PERP","ticker.ETHUSD-PERP","ticker.SOLUSD-PERP","ticker.XRPUSD-PERP","ticker.HYPEUSD-PERP","ticker.DOGEUSD-PERP","mark.BTCUSD-PERP","mark.ETHUSD-PERP","mark.SOLUSD-PERP","mark.XRPUSD-PERP","mark.HYPEUSD-PERP","mark.DOGEUSD-PERP"]},"nonce":%d}
                            """.formatted(nonce);
                    log.info("Crypto.com sending subscribe: {}", subscribe);
                    client.send(subscribe);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Crypto.com subscribe delay interrupted");
            }
        }).start();
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            
            // 记录所有收到的消息以便调试（INFO级别以便排查问题）
            String method = root.has("method") ? root.get("method").asText() : "";
            log.info("Crypto.com received message - method: {}, length: {}", method, message.length());
            if (!method.equals("public/heartbeat") && !method.equals("public/respond-heartbeat")) {
                log.info("Crypto.com message content: {}", message.length() > 500 ? message.substring(0, 500) + "..." : message);
            }
            
            // 处理订阅确认消息 - 但订阅响应可能包含初始数据，需要继续处理
            // 如果method="subscribe"且result中有data，需要继续处理数据，不return
            if (root.has("id") && root.has("method") && "subscribe".equals(root.get("method").asText())) {
                if (root.has("result")) {
                    JsonNode result = root.get("result");
                    // 如果result中有data数组且不为空，说明包含数据，需要继续处理
                    if (result.has("data") && result.get("data").isArray() && result.get("data").size() > 0) {
                        log.info("Crypto.com subscription response with data, will process");
                        // 继续处理，不return
                    } else {
                        // 没有数据，只是订阅确认
                        log.info("Crypto.com subscription confirmed: {}", message);
                        return;
                    }
                } else if (root.has("code") && root.get("code").asInt() == 0) {
                    // 只有code=0，没有result字段，可能是简单的确认
                    log.info("Crypto.com subscription confirmed: {}", message);
                    return;
                }
            }
            
            // 处理服务器发送的心跳请求 - 必须响应，否则连接会在5秒内断开
            if (root.has("method") && "public/heartbeat".equals(root.get("method").asText())) {
                // 服务器每30秒发送心跳，客户端必须在5秒内响应
                Long heartbeatId = root.has("id") ? root.get("id").asLong() : null;
                if (heartbeatId != null && client != null && client.isOpen()) {
                    // 立即响应心跳
                    String response = String.format("{\"id\":%d,\"method\":\"public/respond-heartbeat\"}", heartbeatId);
                    client.send(response);
                } else {
                    log.warn("Crypto.com heartbeat received but cannot respond - id: {}, client: {}", heartbeatId, client != null ? "exists" : "null");
                }
                return;
            }
            
            // 处理心跳响应确认
            if (root.has("method") && "public/respond-heartbeat".equals(root.get("method").asText())) {
                return;
            }
            
            // 处理错误消息
            if (root.has("code")) {
                int code = root.get("code").asInt();
                if (code != 0) {
                    String errorMsg = root.has("message") ? root.get("message").asText() : "unknown";
                    log.warn("Crypto.com API error: code={}, message={}, full message: {}", code, errorMsg, message);
                    return;
                }
            }
            
            // Crypto.com的消息格式：订阅响应在result字段中，更新消息在params字段中
            JsonNode params = root.has("params") ? root.get("params") : null;
            JsonNode result = root.has("result") ? root.get("result") : null;
            
            // 如果params为空但result存在，说明是订阅响应消息，使用result作为params
            if (params == null && result != null) {
                params = result;
            }
            
            if (params == null) {
                return;
            }
            
            // 获取channel - 可能在params.channel, result.channel, 或result.subscription中
            String channel = "";
            if (params.has("channel")) {
                channel = params.get("channel").asText();
            } else if (params.has("subscription")) {
                channel = params.get("subscription").asText();
            } else {
                return;
            }
            
            // 从channel中提取基础channel名称（去掉instrument后缀）
            int dotIndex = channel.indexOf('.');
            String baseChannel = dotIndex > 0 ? channel.substring(0, dotIndex) : channel;
            
            String instrument = params.has("instrument_name") ? params.get("instrument_name").asText() : "";
            
            if (instrument.isEmpty()) {
                // 尝试从channel中提取instrument名称
                if (dotIndex > 0) {
                    instrument = channel.substring(dotIndex + 1);
                }
            }
            
            String symbol = toStdSymbol(instrument);
            if (symbol == null) {
                log.warn("Crypto.com unknown instrument: {}, channel: {}", instrument, channel);
                return;
            }

            // 尝试多种方式获取数据
            JsonNode data = null;
            if (params.has("data")) {
                JsonNode dataNode = params.get("data");
                // 如果data是数组，取第一个元素
                if (dataNode.isArray() && dataNode.size() > 0) {
                    data = dataNode.get(0);
                } else if (!dataNode.isArray()) {
                    data = dataNode;
                }
            } else if (params.has("result")) {
                data = params.get("result");
            } else {
                // 如果params本身就是数据对象（某些情况下）
                data = params;
            }

            boolean hasData = false;
            if (baseChannel.equals("funding") || baseChannel.equals("estimatedfunding") || channel.startsWith("funding.") || channel.startsWith("estimatedfunding.")) {
                if (data != null) {
                    // Crypto.com的funding数据格式：{"v":"0.000018211","t":1771671605000}
                    // v是funding rate值，t是时间戳
                    BigDecimal rate = parseDecimal(data, "v"); // Crypto.com使用"v"字段表示funding rate值
                    if (rate == null) {
                        rate = parseDecimal(data, "funding_rate"); // 也尝试标准字段名
                    }
                    if (rate != null) {
                        Long nextTime = null;
                        if (data.has("funding_interval_end")) {
                            nextTime = data.get("funding_interval_end").asLong() * 1000;
                        } else if (data.has("next_funding_time")) {
                            nextTime = data.get("next_funding_time").asLong();
                        }
                        marketDataService.saveFundingRate("cryptocom", symbol, rate, nextTime);
                        hasData = true;
                        log.info("Crypto.com saved funding rate for {}: {}", symbol, rate);
                    }
                }
            } else if (baseChannel.equals("ticker") || channel.startsWith("ticker.")) {
                if (data != null) {
                    // Crypto.com的ticker数据格式：{"h":"68314.6","l":"66425.7","a":"68196.8","c":"0.0040",...}
                    // a是ask price（卖价），可以用作期货价格
                    BigDecimal last = parseDecimal(data, "a"); // ask price
                    if (last == null) last = parseDecimal(data, "last");
                    if (last == null) last = parseDecimal(data, "k"); // ask
                    if (last == null) last = parseDecimal(data, "c"); // close price
                    if (last == null) last = parseDecimal(data, "b"); // bid price
                    if (last != null) {
                        marketDataService.saveFuturesPrice("cryptocom", symbol, last);
                        hasData = true;
                        log.info("Crypto.com saved futures price (ticker) for {}: {}", symbol, last);
                    }
                    
                    // Crypto.com期货ticker只提供期货价格，不保存现货价格
                    // 现货价格应从index channel获取，如果index channel不可用，则不保存现货价格
                    // 不应使用ticker的mid price作为现货价格，因为这是期货ticker，会导致价差为0
                }
            } else if (baseChannel.equals("mark") || channel.startsWith("mark.")) {
                if (data != null) {
                    // Crypto.com的mark数据格式：{"v":"68195.8","t":1771671645000}
                    // v是mark price值
                    BigDecimal mark = parseDecimal(data, "v"); // Crypto.com使用"v"字段表示mark price值
                    if (mark == null) {
                        mark = parseDecimal(data, "mark_price"); // 也尝试标准字段名
                    }
                    if (mark == null) {
                        mark = parseDecimal(data, "markPrice");
                    }
                    if (mark == null) {
                        mark = parseDecimal(data, "price");
                    }
                    if (mark != null) {
                        marketDataService.saveFuturesPrice("cryptocom", symbol, mark);
                        hasData = true;
                        log.info("Crypto.com saved futures price (mark) for {}: {}", symbol, mark);
                    }
                }
            } else if (baseChannel.equals("index") || channel.startsWith("index.")) {
                if (data != null) {
                    // Crypto.com的index数据格式可能是：{"v":"68195.8","t":1771671645000} 或数组格式
                    BigDecimal index = null;
                    
                    // 如果data是数组，遍历数组查找价格
                    if (data.isArray()) {
                        for (JsonNode item : data) {
                            index = parseDecimal(item, "v");
                            if (index == null) index = parseDecimal(item, "index_price");
                            if (index == null) index = parseDecimal(item, "price");
                            if (index == null) index = parseDecimal(item, "value");
                            if (index != null) break;
                        }
                    } else {
                        // 尝试多个可能的字段名
                        index = parseDecimal(data, "v"); // Crypto.com使用"v"字段表示index price值
                        if (index == null) {
                            index = parseDecimal(data, "index_price");
                        }
                        if (index == null) {
                            index = parseDecimal(data, "price");
                        }
                        if (index == null) {
                            index = parseDecimal(data, "value");
                        }
                        if (index == null) {
                            index = parseDecimal(data, "indexPrice");
                        }
                        if (index == null) {
                            index = parseDecimal(data, "index");
                        }
                    }
                    
                    if (index != null) {
                        marketDataService.saveSpotPrice("cryptocom", symbol, index);
                        hasData = true;
                        log.info("Crypto.com saved spot price (index) for {}: {}", symbol, index);
                    } else {
                        log.warn("Crypto.com index channel data but no valid price found for {}, channel: {}, data: {}", 
                            symbol, channel, data.toString().length() > 300 ? data.toString().substring(0, 300) + "..." : data.toString());
                    }
                } else {
                    log.warn("Crypto.com index channel but data is null, channel: {}", channel);
                }
            }
            
            if (!hasData && data != null) {
                java.util.Iterator<String> fieldNames = data.fieldNames();
                java.util.ArrayList<String> keys = new java.util.ArrayList<>();
                while (fieldNames.hasNext()) {
                    keys.add(fieldNames.next());
                }
                log.warn("Crypto.com channel {} processed but no valid data found, instrument: {}, data keys: {}", 
                    channel, instrument, keys.isEmpty() ? "none" : String.join(", ", keys));
                log.info("Crypto.com data content: {}", data.toString().length() > 300 ? data.toString().substring(0, 300) + "..." : data.toString());
            }
        } catch (Exception e) {
            if (RedisShutdownUtil.isRedisShutdownException(e)) {
                log.debug("Crypto.com parse error (Redis shutdown): {}", e.getMessage());
            } else {
                log.warn("Crypto.com parse error: {} - Message: {}", e.getMessage(),
                    message.length() > 200 ? message.substring(0, 200) + "..." : message);
            }
        }
    }

    // Crypto.com 使用服务器主动心跳机制，不需要客户端主动发送心跳
    // 服务器每30秒发送心跳，客户端必须在5秒内响应
    @Override
    public String getHeartbeatMessage() {
        return null; // 不主动发送心跳，只响应服务器的心跳
    }

    private String toStdSymbol(String instrument) {
        if (instrument == null) return null;
        if (instrument.startsWith("BTC")) return "BTCUSDT";
        if (instrument.startsWith("ETH")) return "ETHUSDT";
        if (instrument.startsWith("SOL")) return "SOLUSDT";
        if (instrument.startsWith("XRP")) return "XRPUSDT";
        if (instrument.startsWith("HYPE")) return "HYPEUSDT";
        if (instrument.startsWith("DOGE")) return "DOGEUSDT";
        if (instrument.startsWith("BNB")) return "BNBUSDT";
        return null;
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
