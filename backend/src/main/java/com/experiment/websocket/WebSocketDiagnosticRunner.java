package com.experiment.websocket;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.context.annotation.DependsOn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * WebSocket 诊断：定期输出各交易所连接状态与最后消息时间，用于判断：
 * - 推送频率是否过低
 * - 是否频繁断连
 * - 是否消息处理失败导致无数据
 */
@Component
@DependsOn("webSocketRunner")
public class WebSocketDiagnosticRunner {

    private static final Logger log = LoggerFactory.getLogger(WebSocketDiagnosticRunner.class);

    private static final long DIAG_INTERVAL_SEC = 30;
    /** 超过该秒数未收到消息且已连接时，记为异常（可能推送频率低或断连后重连中） */
    private static final long STALE_THRESHOLD_SEC = 10;
    /** 有 HTTP 兜底的交易所：WebSocket 长时间无消息时降级为 INFO，不刷 WARN */
    private static final Set<String> HTTP_FALLBACK_EXCHANGES = Set.of("lbank", "bitunix");

    private final WebSocketRunner webSocketRunner;
    private ScheduledExecutorService scheduler;

    public WebSocketDiagnosticRunner(WebSocketRunner webSocketRunner) {
        this.webSocketRunner = webSocketRunner;
    }

    @PostConstruct
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ws-diagnostic");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::runDiagnostic, DIAG_INTERVAL_SEC, DIAG_INTERVAL_SEC, TimeUnit.SECONDS);
        log.info("WebSocket diagnostic started (interval {}s, stale threshold {}s)", DIAG_INTERVAL_SEC, STALE_THRESHOLD_SEC);
    }

    @PreDestroy
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    private void runDiagnostic() {
        for (ManagedWebSocket client : webSocketRunner.getClients()) {
            String name = client.getExchangeName();
            boolean connected = client.isConnected();
            long ageMs = client.getLastMessageAgeMs();
            long ageSec = ageMs < 0 ? -1 : ageMs / 1000;

            if (!connected) {
                log.info("[ws-diagnostic] {} 未连接 (last msg {}s ago)", name, ageSec < 0 ? "never" : ageSec);
            } else if (ageMs < 0 || ageMs > STALE_THRESHOLD_SEC * 1000) {
                String msg = String.format("[ws-diagnostic] %s 已连接但 %ss 未收到消息，可能：交易所推送频率低 / 断连后重连中 / 消息处理失败", name, ageSec < 0 ? "never" : ageSec);
                if (HTTP_FALLBACK_EXCHANGES.contains(name)) {
                    log.info("{}(有HTTP兜底)", msg);
                } else {
                    log.warn("{}", msg);
                }
            } else {
                log.debug("[ws-diagnostic] {} 已连接，上次消息 {}s 前", name, ageSec);
            }
        }
    }
}
