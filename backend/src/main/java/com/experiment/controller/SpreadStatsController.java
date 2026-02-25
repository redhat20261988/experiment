package com.experiment.controller;

import com.experiment.repository.SpreadArbitrageStatsRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 价差套利统计 API：按币种统计各交易所组合出现次数与平均利润率，每币种仅返回次数最高的前 5 个组合。
 */
@RestController
@RequestMapping("/api")
public class SpreadStatsController {

    private final SpreadArbitrageStatsRepository spreadArbitrageStatsRepository;

    public SpreadStatsController(SpreadArbitrageStatsRepository spreadArbitrageStatsRepository) {
        this.spreadArbitrageStatsRepository = spreadArbitrageStatsRepository;
    }

    @GetMapping("/spread-stats")
    public ResponseEntity<Map<String, Object>> getSpreadStats() {
        Map<String, Object> body = Map.of(
                "pairStats",
                spreadArbitrageStatsRepository.findTop5PairStatsGroupBySymbol()
        );
        return ResponseEntity.ok(body);
    }
}
