package com.experiment.websocket;

/**
 * 交易所 WebSocket 消息处理器接口。
 * 各交易所实现此接口，提供连接后的订阅逻辑和消息解析逻辑。
 */
public interface ExchangeWebSocketHandler {

    /**
     * 连接建立成功后调用，用于发送订阅消息。
     *
     * @param client 统一 WebSocket 客户端，用于发送消息
     */
    void onConnected(ManagedWebSocket client);

    /**
     * 收到服务端消息时调用。
     *
     * @param message 原始消息内容
     */
    void onMessage(String message);

    /**
     * 连接关闭时调用。
     *
     * @param code   关闭码
     * @param reason 关闭原因
     */
    default void onClosed(int code, String reason) {
    }

    /**
     * 发生错误时调用。
     *
     * @param ex 异常
     */
    default void onError(Exception ex) {
    }

    /**
     * 获取心跳消息。若不需要应用层心跳则返回 null。
     *
     * @return 心跳消息内容，或 null
     */
    default String getHeartbeatMessage() {
        return null;
    }

    /**
     * 获取心跳发送间隔（毫秒）。仅当 getHeartbeatMessage() 非 null 时生效。
     *
     * @return 间隔毫秒数，默认 30000
     */
    default long getHeartbeatIntervalMs() {
        return 30_000;
    }
}
