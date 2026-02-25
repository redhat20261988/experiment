package com.experiment.util;

/**
 * Redis 连接关闭/停止时的异常检测工具。
 * 应用关闭时 LettuceConnectionFactory 会停止，此时 WebSocket 仍可能尝试写 Redis，
 * 产生 STOPPING/STOPPED/destroyed 等异常，属于正常现象，可降级日志。
 */
public final class RedisShutdownUtil {

    private RedisShutdownUtil() {}

    /**
     * 判断是否为 Redis 关闭/停止期间产生的预期异常。
     * 此类异常在应用重启/关闭时出现，无需按 WARN 记录。
     */
    public static boolean isRedisShutdownException(Throwable t) {
        for (Throwable x = t; x != null; x = x.getCause()) {
            String msg = x.getMessage();
            if (msg != null && (msg.contains("STOPPING") || msg.contains("STOPPED") || msg.contains("destroyed"))) {
                return true;
            }
        }
        return false;
    }
}
