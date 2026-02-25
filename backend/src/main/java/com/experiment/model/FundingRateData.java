package com.experiment.model;

import java.math.BigDecimal;

public record FundingRateData(String exchange, String symbol, BigDecimal rate, Long nextFundingTime) {}
