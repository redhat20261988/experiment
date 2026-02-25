package com.experiment.config;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 各交易所现货/期货手续费率（来自各平台官网，仅供参考，以官网最新为准）。
 * 现货 Maker/Taker 用于价差套利时选「一 maker 一 taker 且总手续费最小」。
 */
public final class ExchangeFeeRates {

    /** 现货 Maker 费率（%），如 0.02 表示 0.02% */
    private static final Map<String, BigDecimal> SPOT_MAKER_PCT = Map.ofEntries(
            Map.entry("binance", new BigDecimal("0.1")),
            Map.entry("okx", new BigDecimal("0.08")),
            Map.entry("bybit", new BigDecimal("0.1")),
            Map.entry("gateio", new BigDecimal("0.2")),
            Map.entry("mexc", BigDecimal.ZERO),
            Map.entry("bitget", new BigDecimal("0.1")),
            Map.entry("coinex", new BigDecimal("0.16")),
            Map.entry("cryptocom", new BigDecimal("0.4")),
            Map.entry("kucoin", new BigDecimal("0.1")),
            Map.entry("htx", new BigDecimal("0.2")),
            Map.entry("bingx", new BigDecimal("0.1")),
            Map.entry("coinw", new BigDecimal("0.1")),
            Map.entry("kraken", new BigDecimal("0.1")),
            Map.entry("bitfinex", new BigDecimal("0.1")),
            Map.entry("hyperliquid", new BigDecimal("0.04")),  // Spot Base Tier 0, https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees
            Map.entry("bitunix", new BigDecimal("0.1")),
            Map.entry("whitebit", new BigDecimal("0.1")),
            Map.entry("lbank", new BigDecimal("0.1")),
            Map.entry("dydx", new BigDecimal("0.01"))   // Tier 1 <$1M, https://docs.dydx.xyz/concepts/trading/rewards
    );

    /** 现货 Taker 费率（%） */
    private static final Map<String, BigDecimal> SPOT_TAKER_PCT = Map.ofEntries(
            Map.entry("binance", new BigDecimal("0.1")),
            Map.entry("okx", new BigDecimal("0.1")),
            Map.entry("bybit", new BigDecimal("0.1")),
            Map.entry("gateio", new BigDecimal("0.2")),
            Map.entry("mexc", new BigDecimal("0.05")),
            Map.entry("bitget", new BigDecimal("0.1")),
            Map.entry("coinex", new BigDecimal("0.16")),
            Map.entry("cryptocom", new BigDecimal("0.4")),
            Map.entry("kucoin", new BigDecimal("0.1")),
            Map.entry("htx", new BigDecimal("0.2")),
            Map.entry("bingx", new BigDecimal("0.1")),
            Map.entry("coinw", new BigDecimal("0.1")),
            Map.entry("kraken", new BigDecimal("0.2")),
            Map.entry("bitfinex", new BigDecimal("0.15")),
            Map.entry("hyperliquid", new BigDecimal("0.07")),  // Spot Base Tier 0
            Map.entry("bitunix", new BigDecimal("0.1")),
            Map.entry("whitebit", new BigDecimal("0.1")),
            Map.entry("lbank", new BigDecimal("0.1")),
            Map.entry("dydx", new BigDecimal("0.05"))   // Tier 1 Taker 5.0 bps
    );

    /** 现货交易手续费率（典型 Taker 或 统一费率，百分比形式如 "0.1%"） */
    private static final Map<String, String> SPOT_FEE = Map.ofEntries(
            Map.entry("binance", "0.1%"),
            Map.entry("okx", "0.08%/0.1%"),
            Map.entry("bybit", "0.1%"),
            Map.entry("gateio", "0.2%"),
            Map.entry("mexc", "0%/0.05%"),
            Map.entry("bitget", "0.1%"),
            Map.entry("coinex", "0.16%"),
            Map.entry("cryptocom", "0.4%"),
            Map.entry("kucoin", "0.1%"),
            Map.entry("htx", "0.2%"),
            Map.entry("bingx", "0.1%"),
            Map.entry("coinw", "0.1%"),
            Map.entry("kraken", "0.1%/0.2%"),
            Map.entry("bitfinex", "0.1%/0.15%"),
            Map.entry("hyperliquid", "0.04%/0.07%"),  // Spot Base Tier 0, 官网按档位不同
            Map.entry("bitunix", "0.1%"),
            Map.entry("whitebit", "0.1%"),
            Map.entry("lbank", "0.1%"),
            Map.entry("dydx", "0.01%/0.05%")  // Tier 1 Maker/Taker bps, 官网按 30d 成交量分档
    );

    /** 期货/永续合约手续费率（典型 Taker 或 Maker/Taker，百分比形式） */
    private static final Map<String, String> FUTURES_FEE = Map.ofEntries(
            Map.entry("binance", "0.02%/0.05%"),
            Map.entry("okx", "0.02%/0.05%"),
            Map.entry("bybit", "0.02%/0.055%"),
            Map.entry("gateio", "0.02%/0.05%"),
            Map.entry("mexc", "0%/0.02%"),
            Map.entry("bitget", "0.02%/0.06%"),
            Map.entry("coinex", "0.03%/0.05%"),
            Map.entry("cryptocom", "0.05%"),
            Map.entry("kucoin", "0.02%/0.06%"),
            Map.entry("htx", "0.02%/0.04%"),
            Map.entry("bingx", "0.02%/0.05%"),
            Map.entry("coinw", "0.02%/0.05%"),
            Map.entry("kraken", "0.02%/0.05%"),
            Map.entry("bitfinex", "0.02%/0.065%"),
            Map.entry("hyperliquid", "0.015%/0.045%"),
            Map.entry("bitunix", "0.02%/0.05%"),
            Map.entry("whitebit", "0.05%/0.05%"),
            Map.entry("lbank", "0.02%/0.05%"),
            Map.entry("dydx", "0.01%/0.05%")  // Fee tiers Tier 1, https://docs.dydx.xyz/concepts/trading/rewards
    );

    public static String getSpotFeeRate(String exchange) {
        return SPOT_FEE.getOrDefault(exchange != null ? exchange.toLowerCase() : "", "-");
    }

    public static String getFuturesFeeRate(String exchange) {
        return FUTURES_FEE.getOrDefault(exchange != null ? exchange.toLowerCase() : "", "-");
    }

    /** 现货 Maker 费率（%），无则返回 null */
    public static BigDecimal getSpotMakerFeePct(String exchange) {
        return SPOT_MAKER_PCT.get(exchange != null ? exchange.toLowerCase() : "");
    }

    /** 现货 Taker 费率（%），无则返回 null */
    public static BigDecimal getSpotTakerFeePct(String exchange) {
        return SPOT_TAKER_PCT.get(exchange != null ? exchange.toLowerCase() : "");
    }

    private ExchangeFeeRates() {}
}
