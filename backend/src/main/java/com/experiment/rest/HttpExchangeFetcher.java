package com.experiment.rest;

/**
 * 交易所 HTTP 数据获取器接口。
 * 用于无 WebSocket 或需补充数据的交易所，每秒轮询获取资金费率与价格。
 */
public interface HttpExchangeFetcher {

    String getExchangeName();

    void fetchAndSave();
}
