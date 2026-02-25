package com.experiment.service;

import com.experiment.config.ExchangeFeeRates;
import com.experiment.model.MarketDataDTO;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class MarketDataService {

    private static final String FUNDING_PREFIX = "funding:";
    private static final String FUTURES_PREFIX = "futures:";
    private static final String SPOT_PREFIX = "spot:";

    /** 所有 Redis 缓存的有效期（秒）。15s 覆盖 Kraken/Hyperliquid 等慢 fetcher 的完整轮询周期（6-10s），减少间歇性空数据 */
    private static final long CACHE_TTL_SECONDS = 15;

    private final RedisTemplate<String, Object> redisTemplate;

    public MarketDataService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void saveFundingRate(String exchange, String symbol, BigDecimal rate, Long nextFundingTime) {
        String key = FUNDING_PREFIX + exchange + ":" + symbol;
        
        // 如果没有提供nextFundingTime，自动计算下一个结算时间（每8小时一次：00:00, 08:00, 16:00 UTC）
        if (nextFundingTime == null && rate != null) {
            nextFundingTime = calculateNextFundingTime();
        }
        
        Map<String, Object> data = new HashMap<>();
        data.put("rate", rate != null ? rate.toString() : null);
        data.put("nextFundingTime", nextFundingTime);
        data.put("updatedAt", System.currentTimeMillis());
        redisTemplate.opsForHash().putAll(key, data.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() != null ? e.getValue().toString() : "")));
        redisTemplate.expire(key, CACHE_TTL_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * 计算下一个资金费率结算时间。
     * 大多数永续合约交易所每8小时结算一次，时间点为UTC时间的00:00, 08:00, 16:00。
     * 
     * @return 下一个结算时间的毫秒时间戳
     */
    private Long calculateNextFundingTime() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        
        // 结算时间点：00:00, 08:00, 16:00 UTC
        int[] fundingHours = {0, 8, 16};
        
        // 找到下一个结算时间点
        for (int hour : fundingHours) {
            ZonedDateTime candidate = now.withHour(hour).withMinute(0).withSecond(0).withNano(0);
            
            // 如果候选时间在今天且还未到，返回它
            if (candidate.isAfter(now)) {
                return candidate.toInstant().toEpochMilli();
            }
        }
        
        // 如果今天的所有结算时间都已过去，返回明天的00:00
        ZonedDateTime tomorrow = now.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        return tomorrow.toInstant().toEpochMilli();
    }

    public void saveFuturesPrice(String exchange, String symbol, BigDecimal price) {
        String key = FUTURES_PREFIX + exchange + ":" + symbol;
        
        Map<String, Object> data = new HashMap<>();
        data.put("price", price != null ? price.toString() : null);
        data.put("updatedAt", System.currentTimeMillis());
        redisTemplate.opsForHash().putAll(key, data.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() != null ? e.getValue().toString() : "")));
        redisTemplate.expire(key, CACHE_TTL_SECONDS, TimeUnit.SECONDS);
    }

    public void saveSpotPrice(String exchange, String symbol, BigDecimal price) {
        String key = SPOT_PREFIX + exchange + ":" + symbol;
        Map<String, Object> data = new HashMap<>();
        data.put("price", price != null ? price.toString() : null);
        data.put("updatedAt", System.currentTimeMillis());
        redisTemplate.opsForHash().putAll(key, data.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() != null ? e.getValue().toString() : "")));
        redisTemplate.expire(key, CACHE_TTL_SECONDS, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public List<MarketDataDTO> getMarketDataBySymbol(String symbol) {
        String symbolUpper = symbol.toUpperCase();
        String symbolKey = symbolUpper + "USDT";

        List<String> exchanges = List.of(
                "binance", "okx", "bybit", "gateio", "mexc", "bitget",
                "coinex", "cryptocom",
                "kucoin", "htx", "bingx", "coinw",
                "kraken", "bitfinex", "hyperliquid", "bitunix",
                "whitebit", "lbank", "dydx"
        );

        List<MarketDataDTO> result = new ArrayList<>();

        for (String exchange : exchanges) {
            BigDecimal fundingRate = getFundingRate(exchange, symbolKey);
            Long nextFundingTime = getNextFundingTime(exchange, symbolKey);
            BigDecimal futuresPrice = getFuturesPrice(exchange, symbolKey);
            BigDecimal spotPrice = getSpotPrice(exchange, symbolKey);

            result.add(new MarketDataDTO(
                    exchange,
                    fundingRate,
                    nextFundingTime,
                    futuresPrice,
                    spotPrice,
                    ExchangeFeeRates.getSpotFeeRate(exchange),
                    ExchangeFeeRates.getFuturesFeeRate(exchange)
            ));
        }

        return result.stream()
                .sorted((a, b) -> {
                    BigDecimal ra = a.fundingRate() != null ? a.fundingRate() : BigDecimal.ZERO;
                    BigDecimal rb = b.fundingRate() != null ? b.fundingRate() : BigDecimal.ZERO;
                    return rb.compareTo(ra);
                })
                .collect(Collectors.toList());
    }

    private BigDecimal getFundingRate(String exchange, String symbol) {
        String key = FUNDING_PREFIX + exchange + ":" + symbol;
        Object val = redisTemplate.opsForHash().get(key, "rate");
        if (val == null || val.toString().isEmpty()) return null;
        try {
            return new BigDecimal(val.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Long getNextFundingTime(String exchange, String symbol) {
        String key = FUNDING_PREFIX + exchange + ":" + symbol;
        Object val = redisTemplate.opsForHash().get(key, "nextFundingTime");
        if (val == null || val.toString().isEmpty()) return null;
        try {
            return Long.parseLong(val.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private BigDecimal getFuturesPrice(String exchange, String symbol) {
        String key = FUTURES_PREFIX + exchange + ":" + symbol;
        Object val = redisTemplate.opsForHash().get(key, "price");
        if (val == null || val.toString().isEmpty()) return null;
        try {
            return new BigDecimal(val.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private BigDecimal getSpotPrice(String exchange, String symbol) {
        String key = SPOT_PREFIX + exchange + ":" + symbol;
        Object val = redisTemplate.opsForHash().get(key, "price");
        if (val == null || val.toString().isEmpty()) return null;
        try {
            return new BigDecimal(val.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
