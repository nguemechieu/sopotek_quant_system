import asyncio

from historical_loader import load_binance_data
from backtest_engine import BacktestEngine
from sopotek_trading.backend.risk.institutional_risk import InstitutionalRiskEngine
from sopotek_trading.backend.strategy.trend_strategy import TrendStrategy


async def main():

    df = load_binance_data("ETC/USDT", "1d", 1000)

    strategy = TrendStrategy()

    risk_engine = InstitutionalRiskEngine(
        account_equity=10000
    )

    engine = BacktestEngine(
        strategy,
        risk_engine
    )

    results = await engine.run(df)

    print(results)




if __name__ == "__main__":

        asyncio.run(main())