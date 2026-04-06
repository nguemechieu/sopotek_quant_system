export const dashboardData = {
  source: "demo",
  totalEquity: 12450000,
  dailyPnl: 345000,
  weeklyPnl: 812000,
  monthlyPnl: 1730000,
  activePositions: 14,
  grossExposure: 9800000,
  netExposure: 4120000,
  var95: 1.84,
  drawdown: 3.2,
  selectedSymbols: ["EUR_USD", "XAU_USD", "BTC_USDT", "NAS100_USD"],
  tradingEnabled: true,
  alerts: [
    { category: "risk", severity: "warning", message: "Energy basket exposure is near the desk cap." },
    { category: "execution", severity: "info", message: "OANDA latency improved after stream rebalance." },
    { category: "strategy", severity: "critical", message: "Event Breakout is paused ahead of CPI." }
  ],
  topStrategies: [
    { name: "Adaptive Trend", sharpe: 1.82, pnl: 412000, status: "enabled" },
    { name: "Mean Reversion FX", sharpe: 1.37, pnl: 265000, status: "enabled" },
    { name: "Event Breakout", sharpe: 1.94, pnl: 149000, status: "paused" }
  ],
  positions: [
    { symbol: "EUR_USD", side: "long", quantity: 300000, avgPrice: 1.0982, markPrice: 1.1024, notionalExposure: 330720, unrealizedPnl: 1260, assetClass: "fx" },
    { symbol: "XAU_USD", side: "long", quantity: 42, avgPrice: 2311.6, markPrice: 2330.4, notionalExposure: 97876.8, unrealizedPnl: 789.6, assetClass: "commodities" },
    { symbol: "BTC_USDT", side: "short", quantity: 18, avgPrice: 70420, markPrice: 69780, notionalExposure: 1256040, unrealizedPnl: 11520, assetClass: "crypto" }
  ]
};

export const marketData = {
  source: "demo",
  symbol: "EUR_USD",
  last: 1.1024,
  changePct: 0.42,
  bid: 1.1023,
  ask: 1.1025,
  volume: 1200000,
  candles: [
    { time: "09:30", open: 1.0984, high: 1.0997, low: 1.0980, close: 1.0992 },
    { time: "10:00", open: 1.0992, high: 1.1006, low: 1.0988, close: 1.1002 },
    { time: "10:30", open: 1.1002, high: 1.1014, low: 1.0999, close: 1.1010 },
    { time: "11:00", open: 1.1010, high: 1.1018, low: 1.1005, close: 1.1012 },
    { time: "11:30", open: 1.1012, high: 1.1028, low: 1.1007, close: 1.1024 },
    { time: "12:00", open: 1.1024, high: 1.1030, low: 1.1017, close: 1.1021 },
    { time: "12:30", open: 1.1021, high: 1.1029, low: 1.1018, close: 1.1026 },
    { time: "13:00", open: 1.1026, high: 1.1033, low: 1.1020, close: 1.1024 }
  ],
  orderBook: {
    bids: [
      { price: 1.1023, size: 18.2 },
      { price: 1.1022, size: 24.4 },
      { price: 1.1021, size: 17.5 },
      { price: 1.1020, size: 31.6 },
      { price: 1.1019, size: 14.1 }
    ],
    asks: [
      { price: 1.1025, size: 19.4 },
      { price: 1.1026, size: 28.1 },
      { price: 1.1027, size: 23.3 },
      { price: 1.1028, size: 34.8 },
      { price: 1.1029, size: 16.9 }
    ]
  },
  watchlist: [
    { symbol: "EUR_USD", last: 1.1024, changePct: 0.42 },
    { symbol: "XAU_USD", last: 2330.4, changePct: 0.88 },
    { symbol: "BTC_USDT", last: 69780, changePct: -0.34 },
    { symbol: "NAS100_USD", last: 18442, changePct: 0.61 }
  ]
};

export const strategyData = [
  {
    id: "adaptive-trend",
    name: "Adaptive Trend",
    code: "adaptive_trend",
    status: "enabled",
    parameters: { timeframe: "5m", lookback: 55, risk_budget_bps: 30 },
    performance: { sharpe: 1.82, pnl: 412000, win_rate: 58 },
    assigned_symbols: ["EUR_USD", "XAU_USD", "BTC_USDT"]
  },
  {
    id: "mean-reversion-fx",
    name: "Mean Reversion FX",
    code: "mean_reversion_fx",
    status: "enabled",
    parameters: { timeframe: "1m", zscore_entry: 2.1, zscore_exit: 0.5 },
    performance: { sharpe: 1.37, pnl: 265000, win_rate: 63 },
    assigned_symbols: ["EUR_USD", "GBP_USD", "USD_JPY"]
  },
  {
    id: "event-breakout",
    name: "Event Breakout",
    code: "event_breakout",
    status: "paused",
    parameters: { timeframe: "15m", atr_multiple: 1.6, confirmation_bars: 2 },
    performance: { sharpe: 1.94, pnl: 149000, win_rate: 47 },
    assigned_symbols: ["NAS100_USD", "SPX500_USD", "XAU_USD"]
  }
];

export const ordersData = [
  { id: "ord-1", order_id: "web-1", symbol: "EUR_USD", side: "buy", status: "working", quantity: 100000, average_price: 1.1021, pnl: 1280, venue: "oanda", created_at: "2026-04-06T13:00:00Z" },
  { id: "ord-2", order_id: "web-2", symbol: "XAU_USD", side: "sell", status: "filled", quantity: 24, average_price: 2328.6, pnl: 4640, venue: "oanda", created_at: "2026-04-06T12:22:00Z" },
  { id: "ord-3", order_id: "web-3", symbol: "BTC_USDT", side: "sell", status: "pending", quantity: 6, average_price: null, pnl: 0, venue: "paper", created_at: "2026-04-06T12:55:00Z" }
];

export const riskData = {
  drawdown: 3.2,
  gross_exposure: 9800000,
  net_exposure: 4120000,
  exposure_by_asset: {
    fx: 3400000,
    crypto: 2800000,
    indices: 2100000,
    commodities: 1500000
  },
  risk_limits: {
    max_position_pct: 0.08,
    max_gross_exposure_pct: 1.8,
    max_drawdown_pct: 0.12,
    daily_loss_limit_pct: 0.03,
    var_limit_pct: 0.02
  },
  trading_enabled: true,
  selected_symbols: ["EUR_USD", "XAU_USD", "BTC_USDT", "NAS100_USD"],
  alerts: [
    { category: "risk", severity: "warning", message: "FX gross exposure reached 71% of allowed limit.", created_at: "2026-04-06T13:05:00Z", payload: {} },
    { category: "liquidity", severity: "info", message: "Gold depth normalized after London fix.", created_at: "2026-04-06T13:11:00Z", payload: {} }
  ],
  updated_at: "2026-04-06T13:12:00Z"
};
