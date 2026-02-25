package com.experiment.model;

import java.math.BigDecimal;

public record PriceData(String exchange, String symbol, BigDecimal price, String type) {}
