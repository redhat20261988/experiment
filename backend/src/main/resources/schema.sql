-- 价差快照表：每秒将扣费后利润率>0.05%的币种、买入/卖出交易所、现货价、价差、扣费后利润率及买卖手续费率写入
CREATE TABLE IF NOT EXISTS spread_arbitrage_snapshots (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL COMMENT '币种',
    exchange_buy VARCHAR(32) NOT NULL COMMENT '买入交易所（低价）',
    exchange_sell VARCHAR(32) NOT NULL COMMENT '卖出交易所（高价）',
    spot_price_buy DECIMAL(20,8) NOT NULL COMMENT '买入方现货价',
    spot_price_sell DECIMAL(20,8) NOT NULL COMMENT '卖出方现货价',
    spot_spread DECIMAL(20,8) NOT NULL COMMENT '现货价差 = sell - buy',
    profit_margin_pct DECIMAL(10,4) NOT NULL COMMENT '扣费后利润率% = 原始价差% - 买入手续费% - 卖出手续费%',
    spot_fee_buy_pct DECIMAL(10,4) NULL COMMENT '买入交易所现货手续费率%',
    spot_fee_sell_pct DECIMAL(10,4) NULL COMMENT '卖出交易所现货手续费率%',
    snapshot_time DATETIME(3) NOT NULL COMMENT '快照时间',
    INDEX idx_symbol_time (symbol, snapshot_time),
    INDEX idx_symbol_pair (symbol, exchange_buy, exchange_sell)
);
