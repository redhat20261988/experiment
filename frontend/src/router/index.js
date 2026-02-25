import { createRouter, createWebHistory } from 'vue-router'
import MarketView from '../views/MarketView.vue'
import ArbitrageView from '../views/ArbitrageView.vue'

const routes = [
  { path: '/', name: 'market', component: MarketView },
  { path: '/arbitrage', name: 'arbitrage', component: ArbitrageView }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
