<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { RouterLink } from 'vue-router'
import { getMarketData } from '../api/market'

const symbols = ['BTC', 'ETH', 'SOL', 'XRP', 'HYPE', 'DOGE', 'BNB']
const selectedSymbol = ref('BTC')
const marketData = ref([])
const loading = ref(true)
const error = ref(null)
let pollInterval = null

// 排序：sortKey 为字段名，sortOrder 为 'asc' | 'desc'
// 默认按资金费率从大到小排序
const sortKey = ref('fundingRate')
const sortOrder = ref('desc')

const columns = [
  { key: 'exchange', label: '交易所', type: 'string' },
  { key: 'fundingRate', label: '资金费率', type: 'number' },
  { key: 'monthlyFundingRate', label: '月资金费率', type: 'number' },
  { key: 'yearlyFundingRate', label: '年资金费率', type: 'number' },
  { key: 'nextFundingTime', label: '下次结算时间', type: 'number' },
  { key: 'futuresPrice', label: '期货价格', type: 'number' },
  { key: 'spotPrice', label: '现货价格', type: 'number' },
  { key: 'spotFeeRate', label: '现货手续费率', type: 'string' },
  { key: 'futuresFeeRate', label: '期货手续费率', type: 'string' },
  { key: 'spread', label: '期货/现货价差', type: 'number' },
  { key: 'basis', label: 'Basis (%)', type: 'number', formula: 'Basis = (期货价格 - 现货价格) / 现货价格 × 100%' }
]

// 将资金费率统一转换为小数形式进行比较（Bitunix返回的是百分比形式，需要除以100）
function normalizeFundingRate(rate, exchange) {
  if (rate == null) return null
  // Bitunix返回的是百分比形式，需要除以100转换为小数形式
  return exchange === 'bitunix' ? Number(rate) / 100 : Number(rate)
}

const sortedMarketData = computed(() => {
  const data = [...marketData.value]
  if (!sortKey.value) return data

  const key = sortKey.value
  const asc = sortOrder.value === 'asc'

  return data.sort((a, b) => {
    let va, vb
    if (key === 'spread') {
      va = a.futuresPrice != null && a.spotPrice != null ? Number(a.futuresPrice) - Number(a.spotPrice) : null
      vb = b.futuresPrice != null && b.spotPrice != null ? Number(b.futuresPrice) - Number(b.spotPrice) : null
    } else if (key === 'basis') {
      va = calculateBasis(a.futuresPrice, a.spotPrice)
      vb = calculateBasis(b.futuresPrice, b.spotPrice)
    } else if (key === 'fundingRate') {
      // 资金费率需要统一转换为小数形式再比较
      va = normalizeFundingRate(a.fundingRate, a.exchange)
      vb = normalizeFundingRate(b.fundingRate, b.exchange)
    } else if (key === 'monthlyFundingRate') {
      va = a.fundingRate != null ? calculateMonthlyFundingRate(a.fundingRate, a.exchange) : null
      vb = b.fundingRate != null ? calculateMonthlyFundingRate(b.fundingRate, b.exchange) : null
    } else if (key === 'yearlyFundingRate') {
      va = a.fundingRate != null ? calculateYearlyFundingRate(a.fundingRate, a.exchange) : null
      vb = b.fundingRate != null ? calculateYearlyFundingRate(b.fundingRate, b.exchange) : null
    } else {
      va = a[key]
      vb = b[key]
    }

    const col = columns.find(c => c.key === key)
    const isNum = col?.type === 'number'

    if (va == null && vb == null) return 0
    // null值始终排在最后，无论升序还是降序
    if (va == null) return 1
    if (vb == null) return -1

    let cmp
    if (isNum) {
      const na = Number(va)
      const nb = Number(vb)
      cmp = na < nb ? -1 : na > nb ? 1 : 0
    } else {
      cmp = String(va).localeCompare(String(vb))
    }
    return asc ? cmp : -cmp
  })
})

function toggleSort(key) {
  if (sortKey.value === key) {
    sortOrder.value = sortOrder.value === 'asc' ? 'desc' : 'asc'
  } else {
    sortKey.value = key
    sortOrder.value = 'asc'
  }
}

function sortIcon(key) {
  if (sortKey.value !== key) return '↕'
  return sortOrder.value === 'asc' ? '↑' : '↓'
}

function formatRate(rate, exchange) {
  if (rate == null) return '-'
  // Bitunix API返回的资金费率已经是百分比形式（已乘以100），不需要再乘以100
  if (exchange === 'bitunix') {
    return Number(rate).toFixed(4) + '%'
  }
  // 其他交易所返回的是小数形式，需要乘以100转换为百分比
  const pct = Number(rate) * 100
  // Kraken 等交易所资金费率可能极小（如 -0.00000002），toFixed(4) 会四舍五入为 0.0000%，需增加小数位
  const decimals = (Math.abs(pct) > 0 && Math.abs(pct) < 0.0001) ? 8 : 4
  return pct.toFixed(decimals) + '%'
}

// 计算月资金费率：假设资金费率周期为8小时，一天结算3次，一个月30天
// 月资金费率 = 30天 * 3次/天 * 资金费率 = 90 * 资金费率
function calculateMonthlyFundingRate(fundingRate, exchange) {
  if (fundingRate == null) return null
  // Bitunix返回的是百分比形式，需要先转换为小数形式再计算
  const rate = exchange === 'bitunix' ? Number(fundingRate) / 100 : Number(fundingRate)
  return rate * 30 * 3
}

// 计算年资金费率：假设资金费率周期为8小时，一天结算3次，一年365天
// 年资金费率 = 365天 * 3次/天 * 资金费率 = 1095 * 资金费率
function calculateYearlyFundingRate(fundingRate, exchange) {
  if (fundingRate == null) return null
  // Bitunix返回的是百分比形式，需要先转换为小数形式再计算
  const rate = exchange === 'bitunix' ? Number(fundingRate) / 100 : Number(fundingRate)
  return rate * 365 * 3
}

function formatMonthlyRate(rate, exchange) {
  if (rate == null) return '-'
  const monthlyRate = calculateMonthlyFundingRate(rate, exchange)
  return (monthlyRate * 100).toFixed(2) + '%'
}

function formatYearlyRate(rate, exchange) {
  if (rate == null) return '-'
  const yearlyRate = calculateYearlyFundingRate(rate, exchange)
  return (yearlyRate * 100).toFixed(2) + '%'
}

function formatPrice(price, exchange, isFutures = false) {
  if (price == null) return '-'
  const n = Number(price)
  return n >= 1000 ? n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : n.toFixed(4)
}

function formatTime(ts) {
  if (ts == null) return '-'
  // 兼容秒级时间戳（< 1e12 视为秒）
  const ms = Number(ts) < 1e12 ? Number(ts) * 1000 : Number(ts)
  return new Date(ms).toLocaleTimeString()
}

function formatSpread(futures, spot, exchange) {
  if (futures == null || spot == null) return '-'
  const diff = Number(futures) - Number(spot)
  const sign = diff >= 0 ? '+' : ''
  return sign + diff.toFixed(2)
}

// Basis = (期货价格 - 现货价格) / 现货价格 * 100%
function calculateBasis(futures, spot) {
  if (futures == null || spot == null || Number(spot) === 0) return null
  return (Number(futures) - Number(spot)) / Number(spot) * 100
}

function formatBasis(futures, spot) {
  const basis = calculateBasis(futures, spot)
  if (basis == null) return '-'
  const sign = basis >= 0 ? '+' : ''
  return sign + basis.toFixed(4) + '%'
}

function exchangeLabel(exchange) {
  const labels = {
    binance: 'Binance',
    okx: 'OKX',
    bybit: 'Bybit',
    gateio: 'Gate.io',
    mexc: 'MEXC',
    bitget: 'Bitget',
    coinex: 'CoinEx',
    cryptocom: 'Crypto.com',
    kucoin: 'Kucoin',
    htx: 'HTX',
    bingx: 'BingX',
    coinw: 'CoinW',
    kraken: 'Kraken',
    bitfinex: 'Bitfinex',
    hyperliquid: 'Hyperliquid',
    bitunix: 'Bitunix',
    whitebit: 'WhiteBIT',
    lbank: 'LBank',
    dydx: 'dYdX'
  }
  return labels[exchange] || exchange
}

async function fetchData() {
  try {
    error.value = null
    const res = await getMarketData(selectedSymbol.value)
    marketData.value = res.data || []
  } catch (e) {
    error.value = e.message || '获取数据失败'
    marketData.value = []
  } finally {
    loading.value = false
  }
}

function startPolling() {
  fetchData()
  pollInterval = setInterval(fetchData, 1000)
}

function stopPolling() {
  if (pollInterval) {
    clearInterval(pollInterval)
    pollInterval = null
  }
}

watch(selectedSymbol, () => {
  loading.value = true
  marketData.value = []
  fetchData()
})

onMounted(() => {
  startPolling()
})

onUnmounted(() => {
  stopPolling()
})
</script>

<template>
  <div class="market-view">
    <header class="header">
      <h1>永续期货资金费率监控</h1>
      <div class="header-actions">
        <RouterLink to="/arbitrage" class="nav-link">套利机会 →</RouterLink>
        <div class="coin-selector">
        <label>币种：</label>
        <select v-model="selectedSymbol">
          <option v-for="s in symbols" :key="s" :value="s">{{ s }}</option>
        </select>
        </div>
      </div>
    </header>

    <div v-if="error" class="error">{{ error }}</div>

    <div class="table-wrap">
      <table class="data-table">
        <thead>
          <tr>
            <th
              v-for="col in columns"
              :key="col.key"
              class="sortable"
              :title="col.formula"
              @click="toggleSort(col.key)"
            >
              {{ col.label }}
              <span class="sort-icon">{{ sortIcon(col.key) }}</span>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="loading && marketData.length === 0">
            <td colspan="11" class="loading">加载中...</td>
          </tr>
          <tr v-else-if="!loading && marketData.length === 0 && !error">
            <td colspan="11" class="empty">暂无数据，请确保后端与 Redis 已启动</td>
          </tr>
          <tr v-else v-for="row in sortedMarketData" :key="row.exchange">
            <td>{{ exchangeLabel(row.exchange) }}</td>
            <td :class="{ positive: row.fundingRate != null && row.fundingRate > 0, negative: row.fundingRate != null && row.fundingRate < 0 }">
              {{ formatRate(row.fundingRate, row.exchange) }}
            </td>
            <td :class="{ positive: row.fundingRate != null && row.fundingRate > 0, negative: row.fundingRate != null && row.fundingRate < 0 }">
              {{ formatMonthlyRate(row.fundingRate, row.exchange) }}
            </td>
            <td :class="{ positive: row.fundingRate != null && row.fundingRate > 0, negative: row.fundingRate != null && row.fundingRate < 0 }">
              {{ formatYearlyRate(row.fundingRate, row.exchange) }}
            </td>
            <td>{{ formatTime(row.nextFundingTime) }}</td>
            <td>
              {{ formatPrice(row.futuresPrice, row.exchange, true) }}
            </td>
            <td>{{ formatPrice(row.spotPrice, row.exchange, false) }}</td>
            <td class="fee-cell">{{ row.spotFeeRate ?? '-' }}</td>
            <td class="fee-cell">{{ row.futuresFeeRate ?? '-' }}</td>
            <td>
              {{ formatSpread(row.futuresPrice, row.spotPrice, row.exchange) }}
            </td>
            <td :class="{ positive: calculateBasis(row.futuresPrice, row.spotPrice) != null && calculateBasis(row.futuresPrice, row.spotPrice) > 0, negative: calculateBasis(row.futuresPrice, row.spotPrice) != null && calculateBasis(row.futuresPrice, row.spotPrice) < 0 }">
              {{ formatBasis(row.futuresPrice, row.spotPrice) }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
.market-view {
  max-width: 1200px;
  margin: 0 auto;
  padding: 24px;
  font-family: system-ui, -apple-system, sans-serif;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 24px;
  flex-wrap: wrap;
  gap: 16px;
  background: #2d2d2d;
  color: #e8e8e8;
  padding: 12px 16px;
  border-radius: 8px;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 16px;
}

.nav-link {
  padding: 8px 12px;
  background: #3a3a3a;
  color: #fff;
  border: 1px solid #555;
  border-radius: 6px;
  text-decoration: none;
  font-size: 0.95rem;
}

.nav-link:hover {
  background: #454545;
  color: #fff;
}

.header h1 {
  margin: 0;
  font-size: 1.5rem;
  font-weight: 600;
}

.coin-selector {
  display: flex;
  align-items: center;
  gap: 8px;
}

.coin-selector label {
  color: #e8e8e8;
  font-size: 1rem;
  font-weight: 500;
}

.coin-selector select {
  padding: 8px 12px;
  font-size: 1rem;
  border: 1px solid #555;
  border-radius: 6px;
  background: #2d2d2d;
  color: #e8e8e8;
  cursor: pointer;
}

.coin-selector select option {
  background: #2d2d2d;
  color: #e8e8e8;
}

.error {
  padding: 12px;
  background: #fee;
  color: #c00;
  border-radius: 6px;
  margin-bottom: 16px;
}

.table-wrap {
  overflow-x: auto;
  border: 1px solid #444;
  border-radius: 8px;
}

.data-table {
  width: 100%;
  border-collapse: collapse;
}

.data-table th,
.data-table td {
  padding: 12px 16px;
  text-align: left;
  border-bottom: 1px solid #444;
}

.data-table th {
  background: #3a3a3a;
  color: #e8e8e8;
  font-weight: 600;
}

.data-table th.sortable {
  cursor: pointer;
  user-select: none;
  white-space: nowrap;
}

.data-table th.sortable:hover {
  background: #454545;
}

.data-table th .sort-icon {
  margin-left: 4px;
  opacity: 0.6;
  font-size: 0.85em;
}

.data-table th.sortable:hover .sort-icon {
  opacity: 1;
}

.data-table tbody tr {
  background: #fff;
}

.data-table td {
  color: #1a1a1a;
}

.data-table tbody tr:hover {
  background: #f5f5f5;
}

.loading,
.empty {
  text-align: center;
  color: #555;
  padding: 24px;
}

.empty {
  font-size: 0.9rem;
}

.positive {
  color: #0a0;
}

.negative {
  color: #c00;
}

.no-funding {
  color: #888;
  font-style: italic;
}

.fee-cell {
  white-space: nowrap;
}
</style>
