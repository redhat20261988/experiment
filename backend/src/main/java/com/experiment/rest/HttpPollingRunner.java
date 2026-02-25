package com.experiment.rest;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.experiment.rest.fetcher.*;
import com.experiment.service.MarketDataService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HTTP 轮询：每个交易所以独立虚拟线程按 1 秒间隔并发拉取，配合 2 秒 Redis TTL 保证数据及时更新。
 */
@Component
public class HttpPollingRunner {

    private static final Logger log = LoggerFactory.getLogger(HttpPollingRunner.class);
    private static final long POLL_INTERVAL_MS = 1000;

    private final MarketDataService marketDataService;
    private final RestTemplate restTemplate;
    private final RestTemplate sslRelaxedRestTemplate;
    private final RestTemplate longTimeoutRestTemplate;
    private final List<HttpExchangeFetcher> fetchers = new ArrayList<>();
    private ExecutorService virtualExecutor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public HttpPollingRunner(MarketDataService marketDataService,
                             RestTemplate restTemplate,
                             @Qualifier("sslRelaxedRestTemplate") RestTemplate sslRelaxedRestTemplate,
                             @Qualifier("longTimeoutRestTemplate") RestTemplate longTimeoutRestTemplate) {
        this.marketDataService = marketDataService;
        this.restTemplate = restTemplate;
        this.sslRelaxedRestTemplate = sslRelaxedRestTemplate;
        this.longTimeoutRestTemplate = longTimeoutRestTemplate;
    }

    @PostConstruct
    public void start() {
        fetchers.add(new BinanceFetcher(marketDataService, restTemplate));
        fetchers.add(new GateFetcher(marketDataService, restTemplate));
        fetchers.add(new CoinExFetcher(marketDataService, restTemplate));
        fetchers.add(new KucoinFetcher(marketDataService, restTemplate));
        fetchers.add(new HtxFetcher(marketDataService, restTemplate));
        fetchers.add(new BingxFetcher(marketDataService, restTemplate));
        fetchers.add(new BybitFetcher(marketDataService, restTemplate));
        fetchers.add(new CoinwFetcher(marketDataService, restTemplate));
        fetchers.add(new CryptoComFetcher(marketDataService, restTemplate));
        fetchers.add(new KrakenFetcher(marketDataService, longTimeoutRestTemplate));
        fetchers.add(new WhiteBITFetcher(marketDataService, restTemplate));
        fetchers.add(new HyperliquidFetcher(marketDataService, sslRelaxedRestTemplate));
        fetchers.add(new BitunixFetcher(marketDataService, restTemplate));
        fetchers.add(new BitfinexFetcher(marketDataService, restTemplate));
        fetchers.add(new LBankFetcher(marketDataService, restTemplate));
        fetchers.add(new DydxFetcher(marketDataService, restTemplate));
        fetchers.add(new BitgetFetcher(marketDataService, restTemplate));
        fetchers.add(new OkxFetcher(marketDataService, restTemplate));
        fetchers.add(new MexcFetcher(marketDataService, restTemplate));

        virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();

        for (HttpExchangeFetcher fetcher : fetchers) {
            virtualExecutor.submit(() -> pollLoop(fetcher));
        }

        log.info("Started HTTP polling for {} exchanges (virtual thread per exchange, interval {}ms)", fetchers.size(), POLL_INTERVAL_MS);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (virtualExecutor != null) {
            virtualExecutor.shutdown();
            try {
                if (!virtualExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    virtualExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                virtualExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void pollLoop(HttpExchangeFetcher fetcher) {
        while (running.get()) {
            try {
                fetcher.fetchAndSave();
            } catch (Exception e) {
                // Ignore poll errors
            }
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
