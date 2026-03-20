import asyncio
import os
import sys
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.panels.runtime_updates import (
    load_persisted_runtime_data,
    refresh_open_orders_async,
    refresh_positions_async,
)


def test_refresh_positions_async_uses_broker_then_updates_views():
    events = {"positions": None, "analysis": 0}

    async def fetch_positions():
        return [{"symbol": "EUR/USD", "amount": 1.0}]

    fake = SimpleNamespace(
        _ui_shutting_down=False,
        controller=SimpleNamespace(broker=SimpleNamespace(fetch_positions=fetch_positions)),
        logger=SimpleNamespace(debug=lambda *_args, **_kwargs: None),
        _portfolio_positions_snapshot=lambda: [],
        _populate_positions_table=lambda positions: events.__setitem__("positions", positions),
        _refresh_position_analysis_window=lambda: events.__setitem__("analysis", events["analysis"] + 1),
    )

    asyncio.run(refresh_positions_async(fake))

    assert fake._latest_positions_snapshot == [{"symbol": "EUR/USD", "amount": 1.0}]
    assert events["positions"] == [{"symbol": "EUR/USD", "amount": 1.0}]
    assert events["analysis"] == 1


def test_refresh_open_orders_async_uses_snapshot_api_when_available():
    events = {"orders": None}

    async def fetch_open_orders_snapshot(symbols=None, limit=None):
        return [{"symbol": "BTC/USDT", "id": "ord-1"}]

    fake = SimpleNamespace(
        _ui_shutting_down=False,
        controller=SimpleNamespace(
            broker=SimpleNamespace(
                fetch_open_orders=lambda **_kwargs: [],
                fetch_open_orders_snapshot=fetch_open_orders_snapshot,
            ),
            symbols=["BTC/USDT"],
        ),
        logger=SimpleNamespace(debug=lambda *_args, **_kwargs: None),
        _current_chart_symbol=lambda: "BTC/USDT",
        _populate_open_orders_table=lambda orders: events.__setitem__("orders", orders),
    )

    asyncio.run(refresh_open_orders_async(fake))

    assert fake._latest_open_orders_snapshot == [{"symbol": "BTC/USDT", "id": "ord-1"}]
    assert events["orders"] == [{"symbol": "BTC/USDT", "id": "ord-1"}]


def test_load_persisted_runtime_data_replays_recent_trades():
    loaded = []

    async def load_recent_trades(limit=None):
        return [{"order_id": "ord-1"}, {"order_id": "ord-2"}]

    fake = SimpleNamespace(
        controller=SimpleNamespace(_load_recent_trades=load_recent_trades),
        MAX_LOG_ROWS=200,
        logger=SimpleNamespace(exception=lambda *_args, **_kwargs: None),
        _update_trade_log=lambda trade: loaded.append(trade),
    )

    asyncio.run(load_persisted_runtime_data(fake))

    assert loaded == [{"order_id": "ord-1"}, {"order_id": "ord-2"}]
