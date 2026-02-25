package com.experiment.websocket;

import com.experiment.service.MarketDataService;
import com.experiment.websocket.handler.*;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class WebSocketRunner {

    private static final Logger log = LoggerFactory.getLogger(WebSocketRunner.class);

    private final MarketDataService marketDataService;
    private final List<ManagedWebSocket> clients = new ArrayList<>();

    public WebSocketRunner(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    @PostConstruct
    public void start() {
        try {
            BinanceHandler binanceHandler = new BinanceHandler(marketDataService);
            clients.add(binanceHandler.createFuturesClient());
            clients.add(binanceHandler.createSpotClient());

            OkxHandler okxHandler = new OkxHandler(marketDataService);
            clients.add(okxHandler.createClient());

            BybitHandler bybitHandler = new BybitHandler(marketDataService);
            clients.add(bybitHandler.createFuturesClient());
            clients.add(bybitHandler.createSpotClient());

            GateHandler gateHandler = new GateHandler(marketDataService);
            clients.add(gateHandler.createClient());

            MexcHandler mexcHandler = new MexcHandler(marketDataService);
            clients.add(mexcHandler.createClient());

            BitgetHandler bitgetHandler = new BitgetHandler(marketDataService);
            clients.add(bitgetHandler.createClient());

            CoinExHandler coinExHandler = new CoinExHandler(marketDataService);
            clients.add(coinExHandler.createClient());

            CryptoComHandler cryptoComHandler = new CryptoComHandler(marketDataService);
            clients.add(cryptoComHandler.createClient());

            HyperliquidHandler hyperliquidHandler = new HyperliquidHandler(marketDataService);
            clients.add(hyperliquidHandler.createClient());

            BitunixHandler bitunixHandler = new BitunixHandler(marketDataService);
            clients.add(bitunixHandler.createClient());

            LBankHandler lBankHandler = new LBankHandler(marketDataService);
            clients.add(lBankHandler.createClient());

            DydxHandler dydxHandler = new DydxHandler(marketDataService);
            clients.add(dydxHandler.createClient());

            BitfinexHandler bitfinexHandler = new BitfinexHandler(marketDataService);
            clients.add(bitfinexHandler.createClient());

            for (ManagedWebSocket client : clients) {
                client.connect();
            }

            log.info("Started {} WebSocket connections (unified client with reconnect)", clients.size());
        } catch (Exception e) {
            log.error("Failed to start WebSocket clients", e);
        }
    }

    /** 供诊断组件使用 */
    public List<ManagedWebSocket> getClients() {
        return clients;
    }

    @PreDestroy
    public void stop() {
        for (ManagedWebSocket client : clients) {
            try {
                client.disconnect();
            } catch (Exception e) {
                log.warn("Error disconnecting WebSocket: {}", e.getMessage());
            }
        }
    }
}
