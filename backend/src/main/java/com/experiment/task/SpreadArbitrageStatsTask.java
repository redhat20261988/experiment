package com.experiment.task;

import com.experiment.config.ExchangeFeeRates;
import com.experiment.model.MarketDataDTO;
import com.experiment.repository.SpreadArbitrageStatsRepository;
import com.experiment.service.MarketDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * 每秒计算现货价差利润率：扣减买入/卖出手续费（一 maker 一 taker 且总手续费最小），
 * 仅将扣费后利润率 &gt; 0.05% 的快照写入 spread_arbitrage_snapshots，并记录买卖手续费率。
 */
@Component
public class SpreadArbitrageStatsTask {

    private static final Logger log = LoggerFactory.getLogger(SpreadArbitrageStatsTask.class);
    private static final List<String> SYMBOLS = List.of("BTC", "ETH", "SOL", "XRP", "HYPE", "DOGE", "BNB");
    /** 扣费后利润率阈值：仅写入大于此值的数据 */
    private static final BigDecimal THRESHOLD_PCT = new BigDecimal("0.05");

    private final MarketDataService marketDataService;
    private final SpreadArbitrageStatsRepository repository;

    public SpreadArbitrageStatsTask(MarketDataService marketDataService,
                                   SpreadArbitrageStatsRepository repository) {
        this.marketDataService = marketDataService;
        this.repository = repository;
    }

    /**
     * 每秒执行一次，将扣费后利润率>0.05%的组合写入快照表。
     */
    @Scheduled(fixedRate = 1000, initialDelay = 10_000)
    public void run() {
        List<SpreadArbitrageStatsRepository.SnapshotRow> rows = new ArrayList<>();
        for (String symbol : SYMBOLS) {
            try {
                collectSnapshots(symbol, rows);
            } catch (Exception e) {
                log.warn("[SpreadArbitrageStats] symbol={} error: {}", symbol, e.getMessage());
            }
        }
        if (!rows.isEmpty()) {
            repository.saveSnapshots(rows);
            log.debug("[SpreadArbitrageStats] saved {} snapshot rows", rows.size());
        }
    }

    /**
     * 仅使用现货价参与价差计算与写入，不使用期货价替代。
     * 若用期货价替代缺失的现货价，会导致 (spot_price_sell - spot_price_buy)/spot_price_buy 与
     * 真实现货价差不一致，profit_margin_pct 与按表内价格重算结果不符，且可能误写入本应过滤的负利润率记录。
     */
    private void collectSnapshots(String symbol, List<SpreadArbitrageStatsRepository.SnapshotRow> out) {
        List<MarketDataDTO> data = marketDataService.getMarketDataBySymbol(symbol);
        List<BigDecimal> prices = new ArrayList<>();
        List<String> exchanges = new ArrayList<>();
        for (MarketDataDTO d : data) {
            BigDecimal spotPrice = d.spotPrice();
            if (spotPrice != null && spotPrice.compareTo(BigDecimal.ZERO) > 0) {
                prices.add(spotPrice);
                exchanges.add(d.exchange());
            }
        }
        int n = prices.size();
        if (n < 2) return;

        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                BigDecimal pi = prices.get(i);
                BigDecimal pj = prices.get(j);
                BigDecimal lo = pi.min(pj);
                BigDecimal hi = pi.max(pj);
                String exchangeBuy = pi.compareTo(pj) <= 0 ? exchanges.get(i) : exchanges.get(j);
                String exchangeSell = pi.compareTo(pj) <= 0 ? exchanges.get(j) : exchanges.get(i);
                BigDecimal spotPriceBuy = pi.compareTo(pj) <= 0 ? pi : pj;
                BigDecimal spotPriceSell = pi.compareTo(pj) <= 0 ? pj : pi;
                BigDecimal spotSpread = spotPriceSell.subtract(spotPriceBuy);

                // 原始价差利润率(%)
                BigDecimal rawMarginPct = hi.subtract(lo).divide(lo, 6, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100));

                // 一 maker 一 taker，且总手续费最小
                BigDecimal makerBuy = ExchangeFeeRates.getSpotMakerFeePct(exchangeBuy);
                BigDecimal takerBuy = ExchangeFeeRates.getSpotTakerFeePct(exchangeBuy);
                BigDecimal makerSell = ExchangeFeeRates.getSpotMakerFeePct(exchangeSell);
                BigDecimal takerSell = ExchangeFeeRates.getSpotTakerFeePct(exchangeSell);
                if (makerBuy == null || takerBuy == null || makerSell == null || takerSell == null) continue;

                BigDecimal totalA = makerBuy.add(takerSell); // 买 maker、卖 taker
                BigDecimal totalB = takerBuy.add(makerSell);  // 买 taker、卖 maker
                BigDecimal feeBuyPct;
                BigDecimal feeSellPct;
                if (totalA.compareTo(totalB) <= 0) {
                    feeBuyPct = makerBuy;
                    feeSellPct = takerSell;
                } else {
                    feeBuyPct = takerBuy;
                    feeSellPct = makerSell;
                }

                BigDecimal profitMarginPct = rawMarginPct.subtract(feeBuyPct).subtract(feeSellPct);
                if (profitMarginPct.compareTo(THRESHOLD_PCT) <= 0) continue;

                out.add(new SpreadArbitrageStatsRepository.SnapshotRow(
                        symbol, exchangeBuy, exchangeSell,
                        spotPriceBuy, spotPriceSell, spotSpread, profitMarginPct, feeBuyPct, feeSellPct
                ));
            }
        }
    }
}
