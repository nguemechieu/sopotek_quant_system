# Brokers And Modes

## Broker Adapters In This Repo

### CCXT Broker
- file: `src/broker/ccxt_broker.py`
- role: generic crypto exchange adapter
- supports market data, order submission, order queries, balances, and open orders where the underlying exchange supports them
- now also carries venue preference handling such as `auto`, `spot`, or `option` when available on the venue

### Oanda Broker
- file: `src/broker/oanda_broker.py`
- role: forex adapter
- modes: `practice` and `live`
- market data currently uses polling in this application
- position/account data is normalized for the position analysis window
- rejected orders such as insufficient-margin responses are surfaced back into the app flow

### Alpaca Broker
- file: `src/broker/alpaca_broker.py`
- role: stock broker adapter
- modes: `paper` and `live`
- typically relevant for stock-oriented balances, positions, and order management

### Paper Broker
- file: `src/broker/paper_broker.py`
- role: local simulated execution path
- uses market data while simulating order handling locally
- best starting point when validating UI, charts, and risk controls

### Stellar Broker
- file: `src/broker/stellar_broker.py`
- role: Stellar offer and market-data adapter
- modes: sandbox-like and live public network behavior based on config
- has its own market-watch differences in the UI

## Broker Selection Flow

`src/broker/broker_factory.py` maps:

- `crypto` -> `CCXTBroker`
- `forex` -> `OandaBroker`
- `stocks` -> `AlpacaBroker`
- `paper` -> `PaperBroker`
- exchange `stellar` -> `StellarBroker`

## Mode Guidance

### Use Paper When
- testing a new symbol list
- validating UI state updates
- testing strategy parameters
- validating screenshots, Telegram, or ChatGPT flows
- checking order-state transitions without risking capital

### Use Practice Or Sandbox When
- validating real broker authentication
- validating symbol permissions and price precision
- validating venue-specific order rules
- confirming broker-side order, position, and open-order tracking

### Use Live Only When
- balances and permissions are confirmed
- the symbol is already validated in paper or practice
- manual order flow has been tested
- risk settings and behavior guard limits are reviewed
- kill switch and recovery behavior are understood

## Order Tracking Behavior

The execution layer can track submitted orders after the initial response. When the broker supports `fetch_order()`, the app can update transitions such as:

- `submitted`
- `open`
- `partially_filled`
- `filled`
- `canceled`
- `rejected`

## Broker-Aware Formatting

Manual trading now uses broker and symbol metadata where available to normalize:

- amount precision
- minimum order size
- entry price precision
- stop-loss precision
- take-profit precision

This matters especially when switching between forex, crypto, and stock-style brokers.

## Broker Capability Awareness

The UI now leans toward broker-aware behavior. In practice this matters for:

- option vs spot venue selection
- orderbook availability
- symbol formatting and precision
- open-order visibility
- position/account metric labels in position analysis

## Operational Caution

Validate these items per broker before trusting live routing:

1. authentication
2. symbol format
3. minimum size and precision
4. available balance or margin
5. order type support
6. open-order query support
7. cancel-order support
8. screenshot and chart workflows for the symbols you actually trade
