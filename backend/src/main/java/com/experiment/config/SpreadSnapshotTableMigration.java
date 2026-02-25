package com.experiment.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

/**
 * 为已存在的 spread_arbitrage_snapshots 表添加 spot_fee_buy_pct、spot_fee_sell_pct 列（仅执行一次）。
 */
@Component
public class SpreadSnapshotTableMigration {

    private static final Logger log = LoggerFactory.getLogger(SpreadSnapshotTableMigration.class);

    private final JdbcTemplate jdbcTemplate;

    public SpreadSnapshotTableMigration(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void addFeeColumnsIfMissing() {
        try {
            List<Map<String, Object>> cols = jdbcTemplate.queryForList(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'spread_arbitrage_snapshots' AND COLUMN_NAME IN ('spot_fee_buy_pct','spot_fee_sell_pct')");
            boolean hasBuy = cols.stream().anyMatch(m -> "spot_fee_buy_pct".equals(m.get("COLUMN_NAME")));
            boolean hasSell = cols.stream().anyMatch(m -> "spot_fee_sell_pct".equals(m.get("COLUMN_NAME")));
            if (!hasBuy) {
                jdbcTemplate.execute("ALTER TABLE spread_arbitrage_snapshots ADD COLUMN spot_fee_buy_pct DECIMAL(10,4) NULL COMMENT '买入方现货手续费率%'");
                log.info("[Migration] Added column spread_arbitrage_snapshots.spot_fee_buy_pct");
            }
            if (!hasSell) {
                jdbcTemplate.execute("ALTER TABLE spread_arbitrage_snapshots ADD COLUMN spot_fee_sell_pct DECIMAL(10,4) NULL COMMENT '卖出方现货手续费率%'");
                log.info("[Migration] Added column spread_arbitrage_snapshots.spot_fee_sell_pct");
            }
        } catch (Exception e) {
            log.debug("[Migration] spread_arbitrage_snapshots fee columns check skipped or failed: {}", e.getMessage());
        }
    }
}
