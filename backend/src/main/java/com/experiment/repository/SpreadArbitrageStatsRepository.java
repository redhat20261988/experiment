package com.experiment.repository;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 价差快照持久化与聚合查询（按币种统计组合出现次数、平均利润率，每币种 Top5）。
 */
@Repository
public class SpreadArbitrageStatsRepository {

    private final JdbcTemplate jdbcTemplate;

    /** 列顺序: 1=symbol, 2=exchange_buy, 3=exchange_sell, 4=spread_count, 5=avg_profit_margin_pct, 6=spot_fee_buy_pct, 7=spot_fee_sell_pct */
    private static final RowMapper<SpreadPairStatRow> PAIR_ROW_MAPPER = (rs, i) -> new SpreadPairStatRow(
            rs.getString(1),
            rs.getString(2),
            rs.getString(3),
            rs.getInt(4),
            rs.getBigDecimal(5),
            rs.getBigDecimal(6),
            rs.getBigDecimal(7)
    );

    public SpreadArbitrageStatsRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 批量写入快照（扣费后利润率>0.05%、含买卖手续费率）。
     */
    public void saveSnapshots(List<SnapshotRow> rows) {
        if (rows.isEmpty()) return;
        LocalDateTime now = LocalDateTime.now();
        String sql = "INSERT INTO spread_arbitrage_snapshots (symbol, exchange_buy, exchange_sell, spot_price_buy, spot_price_sell, spot_spread, profit_margin_pct, spot_fee_buy_pct, spot_fee_sell_pct, snapshot_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                SnapshotRow row = rows.get(i);
                ps.setString(1, row.symbol());
                ps.setString(2, row.exchangeBuy());
                ps.setString(3, row.exchangeSell());
                ps.setBigDecimal(4, row.spotPriceBuy());
                ps.setBigDecimal(5, row.spotPriceSell());
                ps.setBigDecimal(6, row.spotSpread());
                ps.setBigDecimal(7, row.profitMarginPct());
                ps.setBigDecimal(8, row.spotFeeBuyPct());
                ps.setBigDecimal(9, row.spotFeeSellPct());
                ps.setObject(10, now);
            }
            @Override
            public int getBatchSize() {
                return rows.size();
            }
        });
    }

    /**
     * 按币种统计每个交易所组合的出现次数与平均利润率，每币种只取次数最高的前 5 个组合。
     * 先按 (symbol, exchange_buy, exchange_sell) 聚合得到每组合的 cnt，再在应用层按 symbol 取前 5，避免窗口函数歧义。
     */
    public Map<String, List<SpreadPairStatRow>> findTop5PairStatsGroupBySymbol() {
        String sql = "SELECT symbol, exchange_buy, exchange_sell, " +
                "       COUNT(*) AS spread_count, AVG(profit_margin_pct) AS avg_profit_margin_pct, " +
                "       AVG(spot_fee_buy_pct) AS spot_fee_buy_pct, AVG(spot_fee_sell_pct) AS spot_fee_sell_pct " +
                "FROM spread_arbitrage_snapshots " +
                "GROUP BY symbol, exchange_buy, exchange_sell " +
                "ORDER BY symbol, spread_count DESC";
        List<SpreadPairStatRow> all = jdbcTemplate.query(sql, PAIR_ROW_MAPPER);
        // 每组 symbol 只保留前 5 条（已按 spread_count DESC 排序）
        return all.stream()
                .collect(Collectors.groupingBy(SpreadPairStatRow::symbol))
                .entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().stream().limit(5).toList()
                ));
    }

    /** 单条快照写入用（profitMarginPct 为扣费后利润率） */
    public record SnapshotRow(
            String symbol,
            String exchangeBuy,
            String exchangeSell,
            BigDecimal spotPriceBuy,
            BigDecimal spotPriceSell,
            BigDecimal spotSpread,
            BigDecimal profitMarginPct,
            BigDecimal spotFeeBuyPct,
            BigDecimal spotFeeSellPct
    ) {}

    /** 聚合结果：组合出现次数 + 平均利润率 + 买卖手续费（供前端每币种 Top5 展示） */
    public record SpreadPairStatRow(
            String symbol,
            String exchangeBuy,
            String exchangeSell,
            int spreadCount,
            BigDecimal avgProfitMarginPct,
            BigDecimal spotFeeBuyPct,
            BigDecimal spotFeeSellPct
    ) {}
}
