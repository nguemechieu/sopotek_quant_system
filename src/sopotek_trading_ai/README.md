# Sopotek Trading AI

Sopotek Trading AI is a modular, event-driven algorithmic trading platform designed for multi-asset trading across cryptocurrency, forex, and stock markets.

The system supports live trading, backtesting, machine learning strategies, and institutional-grade risk management.

---

## Features

- Multi-asset trading
    - Crypto (Binance, Coinbase, Kraken via CCXT)
    - Forex (OANDA)
    - Stocks (Alpaca)

- Event-Driven Architecture

- Algorithmic Trading Strategies
    - Momentum
    - Mean Reversion
    - Arbitrage

- Machine Learning Models
    - Scikit-Learn
    - XGBoost
    - Hidden Markov Models

- Institutional Risk Management
    - Portfolio exposure limits
    - Position sizing
    - Drawdown protection

- Backtesting Engine

- Real-Time Market Data Streaming

- GUI Trading Terminal (PySide6 + PyQtGraph)

- Strategy Research Tools

---

## Project Structure
# Sopotek Trading AI

Sopotek Trading AI is a modular, event-driven algorithmic trading platform designed for multi-asset trading across cryptocurrency, forex, and stock markets.

The system supports live trading, backtesting, machine learning strategies, and institutional-grade risk management.

---

## Features

- Multi-asset trading
  - Crypto (Binance, Coinbase, Kraken via CCXT)
  - Forex (OANDA)
  - Stocks (Alpaca)

- Event-Driven Architecture

- Algorithmic Trading Strategies
  - Momentum
  - Mean Reversion
  - Arbitrage

- Machine Learning Models
  - Scikit-Learn
  - XGBoost
  - Hidden Markov Models

- Institutional Risk Management
  - Portfolio exposure limits
  - Position sizing
  - Drawdown protection

- Backtesting Engine

- Real-Time Market Data Streaming

- GUI Trading Terminal (PySide6 + PyQtGraph)

- Strategy Research Tools

---

## Project Structure

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/sopotek/sopotek-trading-ai.git
cd sopotek-trading-ai
2. Create a virtual environment
python -m venv .venv

Activate:

Windows

.venv\Scripts\activate

Linux / Mac

source .venv/bin/activate
3. Install dependencies
pip install -r requirements.txt
Configuration

Set environment variables inside .env.

Example:

BINANCE_API_KEY=your_key
BINANCE_SECRET=your_secret

ALPACA_API_KEY=your_key
ALPACA_SECRET=your_secret

OANDA_TOKEN=your_token
OANDA_ACCOUNT=your_account
Running the System
Run Live Trading
python scripts/run_live.py
Run Backtesting
python scripts/run_backtest.py
Train Machine Learning Models
python scripts/train_models.py
Download Market Data
python scripts/download_data.py
Data Pipeline
Exchange Data
    ↓
data/raw
    ↓
data/processed
    ↓
data/features
    ↓
Machine Learning / Strategies
Event Driven Architecture

The system uses an event bus to coordinate components.

Market Data
     ↓
Event Bus
     ↓
Strategy
     ↓
Risk Engine
     ↓
Execution Engine
     ↓
Portfolio Manager

This design enables scalable, asynchronous trading.

Testing

Run all tests using:

pytest tests/
Supported Markets
Market	Broker
Crypto	CCXT
Forex	OANDA
Stocks	Alpaca
Technology Stack

Python

Core Libraries

numpy

pandas

scikit-learn

xgboost

Trading APIs

ccxt

oandapyV20

Async Networking

aiohttp

websockets

GUI

PySide6

PyQtGraph

Database

SQLAlchemy

Roadmap

Future improvements include:

distributed strategy execution

GPU machine learning inference

high frequency trading support

portfolio optimization

reinforcement learning strategies

License

MIT License

Author

Sopotek Technologies




Python 3.11
MIT License
Build Passing

