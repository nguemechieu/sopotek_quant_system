import asyncio
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.app_controller import AppController


class _CleanupTask:
    def __init__(self, sink, name):
        self._sink = sink
        self._name = name
        self._done = False

    def done(self):
        return self._done

    def cancel(self):
        self._done = True
        self._sink.append(self._name)


class _ExecutorRecorder:
    def __init__(self, sink):
        self._sink = sink

    def shutdown(self, wait=False, cancel_futures=True):
        self._sink.append((bool(wait), bool(cancel_futures)))


class _FakeTerminal:
    def __init__(self, calls):
        self.detached_tool_windows = {}
        self._calls = calls
        self._ui_shutting_down = False

    def _disconnect_controller_signals(self):
        self._calls["terminal"].append("disconnect")

    def deleteLater(self):
        self._calls["terminal"].append("delete")


class _FakeTradingSystem:
    def __init__(self, calls):
        self._calls = calls
        self._signal_selection_executor = _ExecutorRecorder(self._calls["signal_shutdown"])

    async def stop(self, wait_for_background_workers=False):
        self._calls["trading_stop"].append(bool(wait_for_background_workers))
        self._shutdown_signal_selection_executor(wait=wait_for_background_workers)

    def _shutdown_signal_selection_executor(self, wait=False):
        executor = getattr(self, "_signal_selection_executor", None)
        if executor is None:
            return
        self._signal_selection_executor = None
        executor.shutdown(wait=wait, cancel_futures=True)


def _make_cleanup_controller(*, telegram_error=None):
    calls = {
        "telegram": [],
        "news": [],
        "cancelled": [],
        "ranking_shutdown": [],
        "signal_shutdown": [],
        "trading_stop": [],
        "progress": [],
        "terminal": [],
        "stack_removed": [],
        "broker": [],
        "emitted": [],
    }
    controller = AppController.__new__(AppController)
    controller.logger = logging.getLogger("test.app_controller.shutdown")
    controller.connected = True
    controller.connection_signal = SimpleNamespace(
        emit=lambda state: calls["emitted"].append(state)
    )

    async def stop_telegram_service():
        calls["telegram"].append("stop")
        if telegram_error is not None:
            raise telegram_error

    async def close_news_service():
        calls["news"].append("close")

    async def close_broker():
        calls["broker"].append("close")

    controller._stop_telegram_service = stop_telegram_service
    controller.news_service = SimpleNamespace(close=close_news_service)
    controller._news_cache = {"cached": 1}
    controller._news_inflight = {"inflight": 1}
    controller._strategy_auto_assignment_task = _CleanupTask(calls["cancelled"], "auto")
    controller._strategy_auto_assignment_deferred_task = _CleanupTask(calls["cancelled"], "deferred")
    controller._strategy_ranking_executor = _ExecutorRecorder(calls["ranking_shutdown"])
    controller._terminal_runtime_restore_task = _CleanupTask(calls["cancelled"], "restore")
    controller.strategy_auto_assignment_enabled = True
    controller.time_frame = "1h"
    controller._update_strategy_auto_assignment_progress = lambda **changes: calls["progress"].append(changes)
    controller._ticker_task = _CleanupTask(calls["cancelled"], "ticker")
    controller._ws_task = _CleanupTask(calls["cancelled"], "ws")
    controller._ws_bus_task = _CleanupTask(calls["cancelled"], "ws_bus")
    controller.ws_bus = object()
    controller.ws_manager = object()
    controller.trading_system = _FakeTradingSystem(calls)
    controller.behavior_guard = object()
    controller._live_agent_decision_events = {"live": 1}
    controller._live_agent_runtime_feed = ["event"]
    controller.terminal = _FakeTerminal(calls)
    controller.stack = SimpleNamespace(removeWidget=lambda widget: calls["stack_removed"].append(widget))
    controller.broker = SimpleNamespace(close=close_broker)
    return controller, calls


def test_run_startup_health_check_pushes_notification_once_for_same_result():
    notifications = []
    controller = AppController.__new__(AppController)
    controller.logger = logging.getLogger("test.app_controller.health")
    controller.symbols = ["BTC/USDT"]
    controller.time_frame = "1h"
    controller.health_check_report = []
    controller.health_check_summary = "Not run"
    controller._startup_health_notification_signature = None
    controller.terminal = SimpleNamespace(
        _push_notification=lambda *args, **kwargs: notifications.append((args, kwargs))
    )

    async def fetch_status():
        return {"broker": "paper", "status": "ok"}

    async def fetch_orderbook(_symbol, limit=10):
        return {"bids": [[1.0, 1.0]], "asks": [[1.1, 1.0]]}

    async def fetch_positions():
        return []

    async def fetch_open_orders(symbol=None, limit=10):
        return []

    async def fetch_ohlcv(symbol, timeframe="1h", limit=50):
        return [[1, 1, 1, 1, 1, 1]]

    controller.broker = SimpleNamespace(
        fetch_status=fetch_status,
        fetch_ohlcv=fetch_ohlcv,
        fetch_orderbook=fetch_orderbook,
        fetch_positions=fetch_positions,
        fetch_open_orders=fetch_open_orders,
    )
    controller.get_broker_capabilities = lambda: {
        "connectivity": True,
        "ticker": True,
        "candles": True,
        "orderbook": True,
        "open_orders": True,
        "positions": True,
        "trading": True,
        "order_tracking": True,
    }
    controller._broker_is_connected = lambda broker=None: True

    async def fetch_balances(_broker=None):
        return {"free": {"USD": 1000.0}}

    async def fetch_ticker(symbol):
        return {"symbol": symbol, "last": 100.0}

    controller._fetch_balances = fetch_balances
    controller._safe_fetch_ticker = fetch_ticker

    asyncio.run(controller.run_startup_health_check())
    asyncio.run(controller.run_startup_health_check())

    assert "pass" in controller.health_check_summary
    assert len(notifications) == 1
    assert notifications[0][0][0] == "Startup health check"


def test_shutdown_for_exit_waits_for_background_workers():
    controller, calls = _make_cleanup_controller()

    asyncio.run(controller.shutdown_for_exit())

    assert calls["ranking_shutdown"] == [(True, True)]
    assert calls["trading_stop"] == [True]
    assert calls["signal_shutdown"] == [(True, True)]
    assert calls["broker"] == ["close"]
    assert calls["emitted"] == ["disconnected"]
    assert calls["terminal"] == ["disconnect", "delete"]
    assert len(calls["stack_removed"]) == 1


def test_cleanup_session_continues_after_step_failure():
    controller, calls = _make_cleanup_controller(telegram_error=RuntimeError("boom"))

    asyncio.run(controller._cleanup_session(stop_trading=True, close_broker=True))

    assert calls["telegram"] == ["stop"]
    assert calls["news"] == ["close"]
    assert calls["ranking_shutdown"] == [(False, True)]
    assert calls["trading_stop"] == [False]
    assert calls["signal_shutdown"] == [(False, True)]
    assert calls["broker"] == ["close"]
    assert calls["terminal"] == ["disconnect", "delete"]
    assert len(calls["stack_removed"]) == 1
    assert controller.trading_system is None
    assert controller.terminal is None
    assert controller.broker is None
    assert controller.ws_bus is None
    assert controller.ws_manager is None
