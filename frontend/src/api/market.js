import axios from 'axios'

const API_BASE = '/api'

export async function getMarketData(symbol) {
  const { data } = await axios.get(`${API_BASE}/market/${symbol}`)
  return data
}

/** 从 MySQL 获取价差套利统计（各币种最新一条：交易所组合数、超 0.1% 占比等） */
export async function getSpreadStats() {
  const { data } = await axios.get(`${API_BASE}/spread-stats`)
  return data
}
