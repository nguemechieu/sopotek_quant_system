# UI Workspace Guide

## Dashboard

The dashboard handles pre-terminal setup:

- broker type and exchange selection
- mode selection
- credentials and account fields
- strategy selection
- profile save and reload
- language changes
- live-vs-paper launch framing
- license status and live-use gating

## Terminal Layout

The terminal is a dock-heavy operator workspace. Evidence in `src/frontend/ui/terminal.py` shows panels and tools for:

- market watch and watchlist prioritization
- charts
- trade log
- open orders
- positions
- AI signal monitor
- recommendations
- strategy scorecard / comparison
- risk heatmap
- system console
- system status
- settings
- journal and analytics tools

## Chart Workspace

### Chart Actions
- open symbols in tabs
- detach current tab
- reattach detached charts
- tile detached chart windows
- cascade detached chart windows
- restore detached chart layouts on restart
- update detached charts with live data

### Chart Trading Actions
- double-click a price level to open a manual trade ticket
- right-click for entry / SL / TP / limit actions
- drag trade levels on the chart while the ticket is open
- capture chart screenshots for Telegram or review workflows

### Toolbar Utilities
- timeframe controls
- status button
- screenshot button
- AI scope selector
- AI activity indicator
- session badge and license badge
- kill switch button

## Manual Order Flow

The manual trade ticket supports:

- symbol, side, type, amount, entry, stop loss, and take profit
- broker-aware formatting for amount and price values
- auto-suggested SL/TP that can still be adjusted manually
- chart-linked trade level sync
- one-click limit submission buttons

## Trade Monitoring

### Trade Log
Shows normalized trade records, keeps columns aligned, and marks source such as `manual`, `bot`, or `chatgpt`.

### Open Orders
Shows exchange-side open orders, mark prices, fill state, status, and estimated PnL where possible.

### Positions
Shows broker positions or tracked portfolio state.

### Position Analysis
Provides a separate analysis window for positions across brokers with broker-aware labels such as equity, NAV, cash, margin, and exposure where available.

## AI And Strategy Views

### AI Signal Monitor
Shows symbol-level signal context and scanning output rather than only raw append-only history.

### Recommendations
Shows why a symbol is recommended for attention or trade consideration.

### Market ChatGPT
Supports app-aware questions, screenshots, voice interaction, Telegram management, and controlled trade/order commands.

Typical prompts include:

- `show commands`
- `show trade history analysis`
- `open position analysis`
- `show telegram status`
- `take a screenshot`

## Performance, Journal, And Review

### Performance
Menu-based analytics summarize equity, profitability, drawdown, execution quality, and strategy behavior.

### Closed Journal And Trade Review
Support trade history inspection, notes, screenshots, outcome tagging, and replay-style review.

### Journal Review
Supports weekly and monthly review patterns.

### Trade Checklist
A dedicated pre-trade and post-trade planning window captures setup, risk, discipline, and sign-off notes.

It can be used as the operator bridge between:

- chart idea formation
- manual trade setup
- journal capture
- weekly or monthly review

## Risk And Safety

### Risk Heatmap
Shows explanatory status messages when no portfolio data or no usable risk values are available.

### Behavior Guard
Surfaces overtrading and protective lock conditions in the runtime workspace.

### System Status And System Health
Surface runtime status, health checks, and mode/account context.

## Settings

The application settings window exposes runtime preferences including:

- strategy selection and parameters
- risk profile selection
- Telegram and OpenAI integration keys
- voice and speech options
- news behavior
- other UI and behavior preferences backed by `QSettings`
