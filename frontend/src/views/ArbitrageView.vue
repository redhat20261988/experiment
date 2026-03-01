<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import { getMarketData, getSpreadStats } from '../api/market'

const router = useRouter()
const symbols = ['BTC', 'ETH', 'SOL', 'XRP', 'HYPE', 'DOGE', 'BNB']
const marketDataBySymbol = ref({})
const pairStats = ref({}) // 每币种次数最高的前5个组合：{ symbol: [{ exchangeBuy, exchangeSell, spreadCount, avgProfitMarginPct, spotFeeBuyPct, spotFeeSellPct }, ...] }
const spreadStatsLoading = ref(false)

/** 按币种顺序排列的交易所组合列表（后端已按价差次数降序） */
const orderedPairStats = computed(() => {
  const map = pairStats.value
  return symbols.filter(s => map[s] && map[s].length).map(symbol => ({ symbol, pairs: map[symbol] }))
})

/** 单表展示：所有币种扁平为行，同一币种多行用于 rowspan 合并 */
const flatSpreadRows = computed(() => {
  const out = []
  for (const item of orderedPairStats.value) {
    const n = item.pairs.length
    item.pairs.forEach((p, i) => {
      out.push({
        symbol: item.symbol,
        symbolRowSpan: n,
        isFirstOfSymbol: i === 0,
        exchangeBuy: p.exchangeBuy,
        exchangeSell: p.exchangeSell,
        spreadCount: p.spreadCount,
        avgProfitMarginPct: p.avgProfitMarginPct,
        spotFeeBuyPct: p.spotFeeBuyPct,
        spotFeeSellPct: p.spotFeeSellPct
      })
    })
  }
  return out
})
const loading = ref(true)
const error = ref(null)
let pollInterval = null
let spreadStatsInterval = null


function normalizeFundingRate(rate, exchange) {
  if (rate == null) return null
  return exchange === 'bitunix' ? Number(rate) / 100 : Number(rate)
}

function formatTrimmedNumber(value, maxDecimals = 8) {
  const n = Number(value)
  if (!Number.isFinite(n)) return '-'
  if (Math.abs(n) < 1e-12) return '0'
  return n.toFixed(maxDecimals).replace(/\.?0+$/, '')
}

/** 解析现货手续费率为 maker/taker（百分比数值，如 0.1 表示 0.1%）。"0.08%/0.1%" 为 maker/taker，单值 "0.1%" 或数字视为 maker=taker，"-" 视为 0 */
function parseSpotFeePct(spotFeeRate) {
  if (spotFeeRate == null || spotFeeRate === '' || spotFeeRate === '-') return { makerPct: 0, takerPct: 0 }
  if (typeof spotFeeRate === 'number' && !Number.isNaN(spotFeeRate)) return { makerPct: spotFeeRate, takerPct: spotFeeRate }
  const s = String(spotFeeRate).trim()
  if (!s) return { makerPct: 0, takerPct: 0 }
  const parts = s.split('/').map(p => parseFloat(String(p).replace('%', '').trim()))
  const valid = parts.filter(n => !Number.isNaN(n))
  if (valid.length === 0) return { makerPct: 0, takerPct: 0 }
  if (valid.length === 1) return { makerPct: valid[0], takerPct: valid[0] }
  return { makerPct: valid[0], takerPct: valid[1] }
}

/** 从市场数据项读取现货手续费率（兼容 spotFeeRate / spot_fee_rate） */
function getSpotFeeRate(d) {
  return d?.spotFeeRate ?? d?.spot_fee_rate ?? null
}

/** 现货价差套利：选择买卖对时满足「一 maker 一 taker」且总手续费最小，利润率 = 价差/买入价 - 买入费率 - 卖出费率 */
function computeMaxSpotSpread(data) {
  let best = null
  let bestNetMarginPct = -Infinity
  for (let i = 0; i < data.length; i++) {
    const pa = data[i].spotPrice != null ? Number(data[i].spotPrice) : null
    if (pa == null) continue
    const feeA = parseSpotFeePct(getSpotFeeRate(data[i]))
    for (let j = 0; j < data.length; j++) {
      if (i === j) continue
      const pb = data[j].spotPrice != null ? Number(data[j].spotPrice) : null
      if (pb == null) continue
      const feeB = parseSpotFeePct(getSpotFeeRate(data[j]))
      const spread = pa - pb
      const buyData = pa >= pb ? data[j] : data[i]
      const sellData = pa >= pb ? data[i] : data[j]
      const buyFee = pa >= pb ? feeB : feeA
      const sellFee = pa >= pb ? feeA : feeB
      const buyPrice = Number(buyData.spotPrice)
      if (buyPrice === 0) continue
      // 不能同时 taker 或同时 maker：两种合法组合取总手续费更小者
      const totalFee1 = buyFee.makerPct + sellFee.takerPct  // 买入 maker，卖出 taker
      const totalFee2 = buyFee.takerPct + sellFee.makerPct  // 买入 taker，卖出 maker
      const totalFeePct = Math.min(totalFee1, totalFee2)
      const useBuyMaker = totalFee1 <= totalFee2
      const buyFeePct = useBuyMaker ? buyFee.makerPct : buyFee.takerPct
      const sellFeePct = useBuyMaker ? sellFee.takerPct : sellFee.makerPct
      const grossMarginPct = (spread / buyPrice) * 100
      const netMarginPct = grossMarginPct - totalFeePct
      if (netMarginPct > bestNetMarginPct) {
        bestNetMarginPct = netMarginPct
        const ra = normalizeFundingRate(sellData.fundingRate, sellData.exchange)
        const rb = normalizeFundingRate(buyData.fundingRate, buyData.exchange)
        const fundingDiff = ra != null && rb != null ? ra - rb : null
        best = {
          buy: buyData,
          sell: sellData,
          spread,
          fundingDiff,
          totalFeePct,
          buyFeePct,
          sellFeePct,
          buyFeeRole: useBuyMaker ? 'maker' : 'taker',
          sellFeeRole: useBuyMaker ? 'taker' : 'maker',
          netMarginPct
        }
      }
    }
  }
  return best
}

// 现货价差套利表格行
const spotTableRows = computed(() => {
  return symbols.map(sym => {
    const data = marketDataBySymbol.value[sym] || []
    const r = computeMaxSpotSpread(data)
    if (!r) return { symbol: sym, row: null }
    return { symbol: sym, row: r }
  })
})

// 最大资金费率组合：每个币种取资金费率最小值为买入，最大值为卖出
const maxFundingComboRows = computed(() => {
  return symbols.map(sym => {
    const data = marketDataBySymbol.value[sym] || []
    const withRate = data
      .map(d => ({ d, r: normalizeFundingRate(d.fundingRate, d.exchange) }))
      .filter(x => x.r != null)
      .sort((a, b) => a.r - b.r)

    if (withRate.length < 2) return { symbol: sym, row: null }

    const buy = withRate[0]
    const sell = withRate[withRate.length - 1]
    const totalYield = sell.r - buy.r

    return {
      symbol: sym,
      row: {
        buy: buy.d,
        sell: sell.d,
        buyRate: buy.r,
        sellRate: sell.r,
        totalYield,
        monthlyYield: totalYield * 90,
        yearlyYield: totalYield * 1095
      }
    }
  })
})

function formatRate(rate, exchange) {
  if (rate == null) return '-'
  const pct = exchange === 'bitunix' ? Number(rate) : Number(rate) * 100
  const formatted = formatTrimmedNumber(pct, 8)
  return formatted === '0' ? '0' : `${formatted}%`
}

function formatRateForMaxFundingCombo(rate, exchange) {
  if (rate == null) return '-'
  const pct = exchange === 'bitunix' ? Number(rate) : Number(rate) * 100
  const formatted = formatTrimmedNumber(pct, 6)
  return formatted === '0' ? '0' : `${formatted}%`
}

function formatPrice(price, isFutures = false) {
  if (price == null) return '-'
  const n = Number(price)
  if (!Number.isFinite(n)) return '-'
  if (isFutures) {
    return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
  }
  if (Math.abs(n) < 1e-12) return '0'
  if (Math.abs(n) >= 1000) {
    return n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 8 })
  }
  return formatTrimmedNumber(n, 8)
}

// 价差格式化：按数值大小动态保留小数位，避免 XRP/DOGE 等低价币显示为 0
function formatSpreadValue(spread) {
  if (spread == null) return '-'
  const n = Number(spread)
  if (Math.abs(n) >= 1) return n.toFixed(2)
  if (Math.abs(n) >= 0.01) return n.toFixed(4)
  return n.toFixed(6)
}

// 利润率 = 价差 / 买入价格（期货或现货）；现货套利时用扣费后利润率 netMarginPct
function formatProfitMargin(spread, buyPrice, netMarginPct) {
  if (netMarginPct != null && !Number.isNaN(netMarginPct)) return netMarginPct.toFixed(4) + '%'
  if (spread == null || buyPrice == null) return '-'
  const bp = Number(buyPrice)
  if (bp === 0) return '-'
  const pct = (Number(spread) / bp) * 100
  return pct.toFixed(4) + '%'
}

/** 手续费百分比显示（现货套利与价差统计共用，如 0.08 表示 0.08%） */
function formatFeePct(pct) {
  if (pct == null || Number.isNaN(Number(pct))) return '-'
  return Number(pct).toFixed(4) + '%'
}

/** 现货套利：手续费 + 角色（Maker/Taker），如 "0.0800% (Maker)" */
function formatFeePctWithRole(pct, role) {
  if (pct == null || Number.isNaN(Number(pct))) return '-'
  const s = Number(pct).toFixed(4) + '%'
  if (!role) return s
  const roleLabel = role.charAt(0).toUpperCase() + role.slice(1).toLowerCase()
  return `${s} (${roleLabel})`
}

function formatFundingSpreadPercent(decimalSpread) {
  if (decimalSpread == null) return '-'
  const pct = Number(decimalSpread) * 100
  const sign = pct > 0 ? '+' : ''
  const formatted = formatTrimmedNumber(pct, 8)
  return formatted === '0' ? '0' : `${sign}${formatted}%`
}

function formatYieldPercentFixed2NoPlus(decimalSpread) {
  if (decimalSpread == null) return '-'
  const pct = Number(decimalSpread) * 100
  if (!Number.isFinite(pct)) return '-'
  if (Math.abs(pct) < 1e-12) return '0'
  return `${pct.toFixed(2)}%`
}

function formatYieldPercentFixed6NoPlus(decimalSpread) {
  if (decimalSpread == null) return '-'
  const pct = Number(decimalSpread) * 100
  if (!Number.isFinite(pct)) return '-'
  if (Math.abs(pct) < 1e-12) return '0'
  return `${pct.toFixed(6)}%`
}

function exchangeLabel(exchange) {
  const labels = {
    binance: 'Binance', okx: 'OKX', bybit: 'Bybit', gateio: 'Gate.io', mexc: 'MEXC',
    bitget: 'Bitget', coinex: 'CoinEx', cryptocom: 'Crypto.com', kucoin: 'Kucoin',
    htx: 'HTX', bingx: 'BingX', coinw: 'CoinW', kraken: 'Kraken', bitfinex: 'Bitfinex',
    hyperliquid: 'Hyperliquid', bitunix: 'Bitunix', whitebit: 'WhiteBIT', lbank: 'LBank', dydx: 'dYdX'
  }
  return labels[exchange] || exchange
}

function goToMarket() {
  router.push('/')
}

async function fetchSpreadStats() {
  try {
    spreadStatsLoading.value = true
    const res = await getSpreadStats()
    pairStats.value = res?.pairStats ?? {}
  } catch (e) {
    pairStats.value = {}
  } finally {
    spreadStatsLoading.value = false
  }
}

async function fetchData() {
  try {
    error.value = null
    const res = await Promise.all(symbols.map(s => getMarketData(s)))
    const next = {}
    symbols.forEach((s, i) => {
      next[s] = res[i]?.data ?? []
    })
    marketDataBySymbol.value = next
  } catch (e) {
    error.value = e.message || '获取数据失败'
    marketDataBySymbol.value = {}
  } finally {
    loading.value = false
  }
}

function startPolling() {
  fetchData()
  pollInterval = setInterval(fetchData, 1000)
  fetchSpreadStats()
  spreadStatsInterval = setInterval(fetchSpreadStats, 30_000)
}

function stopPolling() {
  if (pollInterval) {
    clearInterval(pollInterval)
    pollInterval = null
  }
  if (spreadStatsInterval) {
    clearInterval(spreadStatsInterval)
    spreadStatsInterval = null
  }
}

onMounted(() => startPolling())
onUnmounted(() => stopPolling())
</script>

<template>
  <div class="arb-view">
    <header class="header">
      <div class="header-left">
        <button class="nav-link" @click="goToMarket">← 返回资金费率监控</button>
        <h1>套利机会</h1>
      </div>
    </header>

    <div v-if="error" class="error">{{ error }}</div>
    <div v-if="loading && Object.keys(marketDataBySymbol).length === 0" class="loading">加载中...</div>

    <template v-else>
      <!-- 最大资金费率组合 -->
      <section class="arb-section">
        <h2 class="arb-title">最大资金费率组合</h2>
        <div class="table-wrap">
          <table class="data-table arb-table funding-arb-table">
            <thead>
              <tr>
                <th>币种</th>
                <th>买入交易所</th>
                <th>买入交易所费率</th>
                <th>买入期货价格</th>
                <th>卖出交易所</th>
                <th>卖出交易所费率</th>
                <th>卖出期货价格</th>
                <th>8小时总收益率</th>
                <th>月总收益率</th>
                <th>年总收益率</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in maxFundingComboRows" :key="'max-funding-' + item.symbol">
                <td v-if="!item.row" :colspan="10" class="empty-row">{{ item.symbol }} 暂无足够数据</td>
                <template v-else>
                  <td class="symbol-cell">{{ item.symbol }}</td>
                  <td>{{ exchangeLabel(item.row.buy.exchange) }}</td>
                  <td :class="{ positive: item.row.buyRate > 0, negative: item.row.buyRate < 0 }">
                    {{ formatRateForMaxFundingCombo(item.row.buy.fundingRate, item.row.buy.exchange) }}
                  </td>
                  <td>{{ formatPrice(item.row.buy.futuresPrice, true) }}</td>
                  <td>{{ exchangeLabel(item.row.sell.exchange) }}</td>
                  <td :class="{ positive: item.row.sellRate > 0, negative: item.row.sellRate < 0 }">
                    {{ formatRateForMaxFundingCombo(item.row.sell.fundingRate, item.row.sell.exchange) }}
                  </td>
                  <td>{{ formatPrice(item.row.sell.futuresPrice, true) }}</td>
                  <td class="spread-cell" :class="{ positive: item.row.totalYield > 0, negative: item.row.totalYield < 0 }">
                    {{ formatYieldPercentFixed6NoPlus(item.row.totalYield) }}
                  </td>
                  <td class="spread-cell" :class="{ positive: item.row.monthlyYield > 0, negative: item.row.monthlyYield < 0 }">
                    {{ formatYieldPercentFixed2NoPlus(item.row.monthlyYield) }}
                  </td>
                  <td class="spread-cell" :class="{ positive: item.row.yearlyYield > 0, negative: item.row.yearlyYield < 0 }">
                    {{ formatYieldPercentFixed2NoPlus(item.row.yearlyYield) }}
                  </td>
                </template>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <!-- 现货价差套利 -->
      <section class="arb-section">
        <h2 class="arb-title">现货价差套利</h2>
        <div class="table-wrap">
          <table class="data-table arb-table futures-spot-arb-table">
            <thead>
              <tr>
                <th rowspan="2">币种</th>
                <th colspan="2">买入交易所</th>
                <th colspan="2">卖出交易所</th>
                <th rowspan="2">价差</th>
                <th rowspan="2" class="col-spot-fee-buy">买入手续费率</th>
                <th rowspan="2" class="col-spot-fee-sell">卖出手续费率</th>
                <th rowspan="2" class="col-spot-margin">利润率（扣费后）</th>
              </tr>
              <tr class="sub-header">
                <th>交易所</th>
                <th>现货价格</th>
                <th>交易所</th>
                <th>现货价格</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in spotTableRows" :key="item.symbol">
                <td v-if="!item.row" :colspan="9" class="empty-row">{{ item.symbol }} 暂无足够数据</td>
                <template v-else>
                  <td class="symbol-cell">{{ item.symbol }}</td>
                  <td>{{ exchangeLabel(item.row.buy.exchange) }}</td>
                  <td>{{ formatPrice(item.row.buy.spotPrice) }}</td>
                  <td>{{ exchangeLabel(item.row.sell.exchange) }}</td>
                  <td>{{ formatPrice(item.row.sell.spotPrice) }}</td>
                  <td class="spread-cell">{{ formatSpreadValue(item.row.spread) }}</td>
                  <td class="spread-cell col-spot-fee-buy">{{ formatFeePctWithRole(item.row.buyFeePct, item.row.buyFeeRole) }}</td>
                  <td class="spread-cell col-spot-fee-sell">{{ formatFeePctWithRole(item.row.sellFeePct, item.row.sellFeeRole) }}</td>
                  <td class="spread-cell col-spot-margin">{{ formatProfitMargin(item.row.spread, item.row.buy.spotPrice, item.row.netMarginPct) }}</td>
                </template>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <!-- 价差统计：每秒快照写入 MySQL，接口按币种统计组合次数与平均利润率，每币种仅展示次数最高的前 5 个组合 -->
      <section class="arb-section spread-stats-section">
        <h2 class="arb-title">价差统计（MySQL）</h2>
        <p class="arb-summary">每秒将价差利润率 &gt; 0.1% 的币种、买入/卖出交易所及现货价/价差/利润率写入快照表；下表为各币种出现次数最高的前 5 个交易所组合及平均利润率。</p>
        <div class="pair-stats-blocks">
          <div class="table-wrap">
            <table class="data-table arb-table spread-stats-table pair-detail-table merged-table">
              <thead>
                <tr>
                  <th>币种</th>
                  <th>买入交易所</th>
                  <th>卖出交易所</th>
                  <th>买入手续费</th>
                  <th>卖出手续费</th>
                  <th>价差次数</th>
                  <th>平均利润率 (%)</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, idx) in flatSpreadRows" :key="row.symbol + '-' + row.exchangeBuy + '-' + row.exchangeSell + '-' + idx">
                  <td v-if="row.isFirstOfSymbol" :rowspan="row.symbolRowSpan" class="symbol-cell symbol-cell-merged">{{ row.symbol }}</td>
                  <td>{{ exchangeLabel(row.exchangeBuy) }}</td>
                  <td>{{ exchangeLabel(row.exchangeSell) }}</td>
                  <td class="spread-cell">{{ formatFeePct(row.spotFeeBuyPct) }}</td>
                  <td class="spread-cell">{{ formatFeePct(row.spotFeeSellPct) }}</td>
                  <td class="spread-cell">{{ row.spreadCount }}</td>
                  <td class="spread-cell">{{ row.avgProfitMarginPct != null ? Number(row.avgProfitMarginPct).toFixed(4) : '-' }}%</td>
                </tr>
              </tbody>
            </table>
          </div>
          <p v-if="spreadStatsLoading" class="arb-summary">加载中...</p>
          <p v-else-if="flatSpreadRows.length === 0" class="arb-summary">暂无数据（请确保后端与 MySQL 已启动，约 10 秒后开始写入快照，运行一段时间后会出现各币种前 5 组合）</p>
        </div>
      </section>
    </template>
  </div>
</template>

<style scoped>
.arb-view {
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
}

.header-left {
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
  cursor: pointer;
  font-size: 0.95rem;
}

.nav-link:hover {
  background: #454545;
}

.header h1 {
  margin: 0;
  font-size: 1.5rem;
  font-weight: 600;
}

.error {
  padding: 12px;
  background: #fee;
  color: #c00;
  border-radius: 6px;
  margin-bottom: 16px;
}

.loading {
  text-align: center;
  color: #555;
  padding: 24px;
}

.arb-section {
  margin-top: 32px;
}

.arb-title {
  font-size: 1.2rem;
  font-weight: 600;
  margin: 0 0 12px 0;
  color: #e8e8e8;
}

.sub-header th {
  font-weight: 500;
  font-size: 0.9em;
  background: #454545;
}

.symbol-cell {
  font-weight: 600;
}

.spread-cell {
  white-space: nowrap;
}

/* 资金费率之差、月度、年度列宽度减10%（12.15% * 0.9 ≈ 10.94%） */
.col-funding-diff {
  width: 10.94%;
}

/* 资金费率、期货价格列接收多出的宽度 */
.col-funding-rate,
.col-futures-price {
  width: 10.5%;
}

.empty-row {
  color: #888;
  font-style: italic;
}

.table-wrap {
  overflow-x: auto;
  border: 1px solid #444;
  border-radius: 8px;
}

.data-table {
  width: 1000px;
  table-layout: fixed;
  border-collapse: collapse;
}

.funding-arb-table {
  width: 1200px;
}

/* 价差统计（MySQL）表格填满容器，消除右侧空白 */
.spread-stats-table {
  width: 100%;
  min-width: 640px;
}

.pair-stats-blocks {
  margin-top: 24px;
}

.pair-stats-subtitle {
  font-size: 1rem;
  font-weight: 500;
  color: #b0b0b0;
  margin: 0 0 16px 0;
}

.pair-stats-block {
  margin-bottom: 20px;
}

.pair-stats-symbol {
  font-size: 1.05rem;
  font-weight: 600;
  color: #e8e8e8;
  margin: 0 0 8px 0;
}

.pair-detail-table {
  margin-bottom: 8px;
}

/* 单表合并时币种列多行合并，内容垂直居中 */
.symbol-cell-merged {
  vertical-align: middle;
}

/* 期货价差、现货价差表格填满容器，避免最后列右侧空白 */
.futures-spot-arb-table {
  width: 100%;
  min-width: 720px;
}

/* 现货价差套利：买入/卖出手续费率、利润率（扣费后）列宽度 10% */
.futures-spot-arb-table .col-spot-fee-buy,
.futures-spot-arb-table .col-spot-fee-sell,
.futures-spot-arb-table .col-spot-margin {
  width: 10%;
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

.data-table tbody tr {
  background: #fff;
}

.data-table td {
  color: #1a1a1a;
}

.data-table tbody tr:hover {
  background: #f5f5f5;
}

.arb-empty {
  text-align: center;
  color: #555;
  padding: 24px;
  font-size: 0.9rem;
}

.arb-summary {
  margin: 12px 0 0 0;
  font-size: 0.95rem;
  color: #b0b0b0;
}

.positive { color: #0a0; }
.negative { color: #c00; }
</style>
