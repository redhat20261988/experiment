package com.experiment.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统一 WebSocket 客户端，负责连接、断线检测、重连与心跳。
 */
public class ManagedWebSocket {

    private static final Logger log = LoggerFactory.getLogger(ManagedWebSocket.class);

    private static final long INITIAL_RECONNECT_DELAY_MS = 1_000;
    private static final long MAX_RECONNECT_DELAY_MS = 60_000;
    private static final double RECONNECT_BACKOFF_MULTIPLIER = 2.0;

    private final String exchangeName;
    private final URI uri;
    private final ExchangeWebSocketHandler handler;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ws-reconnect");
        t.setDaemon(true);
        return t;
    });

    private volatile WebSocketConnection connection;
    private volatile long nextReconnectDelayMs = INITIAL_RECONNECT_DELAY_MS;
    private volatile ScheduledFuture<?> reconnectFuture;
    private volatile ScheduledFuture<?> heartbeatFuture;
    private final AtomicBoolean running = new AtomicBoolean(true);
    /** 最后收到消息的时间（毫秒），用于诊断推送频率与断连 */
    private final AtomicLong lastMessageTimeMs = new AtomicLong(0);

    public ManagedWebSocket(String exchangeName, URI uri, ExchangeWebSocketHandler handler) {
        this.exchangeName = exchangeName;
        this.uri = uri;
        this.handler = handler;
    }

    public void connect() {
        if (!running.get()) return;
        try {
            connection = new WebSocketConnection(uri, this);
            connection.connect();
        } catch (Exception e) {
            log.error("[{}] 连接失败: {}", exchangeName, e.getMessage());
            scheduleReconnect();
        }
    }

    public void disconnect() {
        running.set(false);
        cancelReconnect();
        cancelHeartbeat();
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    public void send(String text) {
        WebSocketConnection conn = connection;
        if (conn != null && conn.isOpen()) {
            conn.send(text);
        } else {
        }
    }

    public boolean isOpen() {
        WebSocketConnection conn = connection;
        return conn != null && conn.isOpen();
    }

    public String getExchangeName() {
        return exchangeName;
    }

    void onConnectionOpened(WebSocketConnection conn) {
        nextReconnectDelayMs = INITIAL_RECONNECT_DELAY_MS;
        handler.onConnected(this);
        startHeartbeat();
    }

    void onConnectionClosed(WebSocketConnection conn, int code, String reason) {
        connection = null;
        cancelHeartbeat();
        handler.onClosed(code, reason);
        // 1000=正常关闭, 1006=异常关闭(如服务器超时断开)，均有自动重连，INFO 级别即可
        log.info("[{}] 连接关闭: {} - {} (将重连)", exchangeName, code, reason);
        if (running.get()) {
            scheduleReconnect();
        }
    }

    void onMessage(String message) {
        lastMessageTimeMs.set(System.currentTimeMillis());
        handler.onMessage(message);
    }

    /** 诊断：是否已连接 */
    public boolean isConnected() {
        return isOpen();
    }

    /** 诊断：距上次收到消息的毫秒数，-1 表示从未收到 */
    public long getLastMessageAgeMs() {
        long t = lastMessageTimeMs.get();
        return t == 0 ? -1 : System.currentTimeMillis() - t;
    }

    void onError(Exception ex) {
        log.error("[{}] WebSocket 错误", exchangeName, ex);
        handler.onError(ex);
        if (!isOpen() && running.get()) {
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (!running.get() || reconnectFuture != null) return;
        log.info("[{}] {}ms 后重连", exchangeName, nextReconnectDelayMs);
        reconnectFuture = scheduler.schedule(() -> {
            reconnectFuture = null;
            if (running.get()) {
                connect();
            }
        }, nextReconnectDelayMs, TimeUnit.MILLISECONDS);
        nextReconnectDelayMs = Math.min(
                (long) (nextReconnectDelayMs * RECONNECT_BACKOFF_MULTIPLIER),
                MAX_RECONNECT_DELAY_MS
        );
    }

    private void cancelReconnect() {
        if (reconnectFuture != null) {
            reconnectFuture.cancel(false);
            reconnectFuture = null;
        }
    }

    private void startHeartbeat() {
        String msg = handler.getHeartbeatMessage();
        if (msg == null) return;
        long interval = handler.getHeartbeatIntervalMs();
        cancelHeartbeat();
        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
            if (running.get() && isOpen()) {
                send(msg);
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    private void cancelHeartbeat() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
            heartbeatFuture = null;
        }
    }

    /**
     * 底层 WebSocket 连接，委托给 org.java_websocket
     */
    static class WebSocketConnection extends org.java_websocket.client.WebSocketClient {

        private final ManagedWebSocket manager;

        WebSocketConnection(URI uri, ManagedWebSocket manager) {
            super(uri);
            this.manager = manager;
            // Enable permessage-deflate compression for CoinEx (required by their API)
            if ("coinex".equals(manager.exchangeName)) {
                try {
                    // Request permessage-deflate extension in the handshake
                    this.addHeader("Sec-WebSocket-Extensions", "permessage-deflate");
                } catch (Exception e) {
                    // If header setting fails, log but continue
                }
            }
        }

        @Override
        public void onOpen(org.java_websocket.handshake.ServerHandshake handshake) {
            manager.onConnectionOpened(this);
        }

        @Override
        public void onMessage(String message) {
            manager.onMessage(message);
        }

        @Override
        public void onMessage(java.nio.ByteBuffer bytes) {
            // Handle binary messages (potentially gzip compressed)
            // For CoinEx, binary messages might be gzip compressed
            if ("coinex".equals(manager.exchangeName)) {
                try {
                    java.util.zip.GZIPInputStream gzip = new java.util.zip.GZIPInputStream(
                        new java.io.ByteArrayInputStream(bytes.array(), bytes.position(), bytes.remaining()));
                    java.io.BufferedReader reader = new java.io.BufferedReader(
                        new java.io.InputStreamReader(gzip, java.nio.charset.StandardCharsets.UTF_8));
                    StringBuilder decompressed = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        decompressed.append(line);
                    }
                    manager.onMessage(decompressed.toString());
                    return;
                } catch (Exception e) {
                    // If decompression fails, try to decode as UTF-8 string
                    try {
                        String message = new String(bytes.array(), bytes.position(), bytes.remaining(), java.nio.charset.StandardCharsets.UTF_8);
                        manager.onMessage(message);
                    } catch (Exception e2) {
                    }
                }
            } else {
                // For other exchanges, decode as UTF-8 string
                try {
                    String message = new String(bytes.array(), bytes.position(), bytes.remaining(), java.nio.charset.StandardCharsets.UTF_8);
                    manager.onMessage(message);
                } catch (Exception e) {
                }
            }
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            manager.onConnectionClosed(this, code, reason);
        }

        @Override
        public void onError(Exception ex) {
            manager.onError(ex);
        }
    }
}
