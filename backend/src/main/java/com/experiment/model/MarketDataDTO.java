package com.experiment.model;

import java.math.BigDecimal;

public record MarketDataDTO(
        String exchange,
        BigDecimal fundingRate,
        Long nextFundingTime,
        BigDecimal futuresPrice,
        BigDecimal spotPrice,
        String spotFeeRate,
        String futuresFeeRate
) {}
