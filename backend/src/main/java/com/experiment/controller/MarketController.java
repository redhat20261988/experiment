package com.experiment.controller;

import com.experiment.model.MarketDataDTO;
import com.experiment.service.MarketDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class MarketController {

    private static final Logger log = LoggerFactory.getLogger(MarketController.class);
    private final MarketDataService marketDataService;

    public MarketController(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    @GetMapping("/market/{symbol}")
    public ResponseEntity<Map<String, Object>> getMarketData(@PathVariable String symbol) {
        List<MarketDataDTO> data = marketDataService.getMarketDataBySymbol(symbol);
        // 记录空数据的交易所，便于排查
        List<String> emptyFunding = new ArrayList<>();
        List<String> emptyFutures = new ArrayList<>();
        List<String> emptySpot = new ArrayList<>();
        for (MarketDataDTO d : data) {
            if (d.fundingRate() == null) emptyFunding.add(d.exchange());
            if (d.futuresPrice() == null) emptyFutures.add(d.exchange());
            if (d.spotPrice() == null) emptySpot.add(d.exchange());
        }
        log.info("[api/market/{}] emptyFunding={}, emptyFutures={}, emptySpot={}",
                symbol.toUpperCase(), emptyFunding, emptyFutures, emptySpot);
        return ResponseEntity.ok(Map.of(
                "symbol", symbol.toUpperCase(),
                "data", data
        ));
    }
}
