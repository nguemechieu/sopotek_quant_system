import asyncio


async def refresh_positions_async(terminal):
    if terminal._ui_shutting_down:
        return
    broker = getattr(terminal.controller, "broker", None)
    positions = []
    if broker is not None and hasattr(broker, "fetch_positions"):
        try:
            positions = await broker.fetch_positions()
        except Exception as exc:
            terminal.logger.debug("Positions refresh failed: %s", exc)

    if not positions:
        positions = terminal._portfolio_positions_snapshot()

    terminal._latest_positions_snapshot = positions or []
    terminal._populate_positions_table(terminal._latest_positions_snapshot)
    terminal._refresh_position_analysis_window()


def schedule_positions_refresh(terminal):
    task = getattr(terminal, "_positions_refresh_task", None)
    if task is not None and not task.done():
        return

    try:
        terminal._positions_refresh_task = asyncio.get_event_loop().create_task(terminal._refresh_positions_async())
    except Exception as exc:
        terminal.logger.debug("Unable to schedule positions refresh: %s", exc)


async def refresh_open_orders_async(terminal):
    if terminal._ui_shutting_down:
        return
    broker = getattr(terminal.controller, "broker", None)
    orders = []
    if broker is not None and hasattr(broker, "fetch_open_orders"):
        try:
            snapshot = getattr(broker, "fetch_open_orders_snapshot", None)
            if callable(snapshot):
                orders = await snapshot(symbols=getattr(terminal.controller, "symbols", []), limit=200)
            else:
                active_symbol = str(terminal._current_chart_symbol() or "").strip()
                request = {"limit": 200}
                if active_symbol:
                    request["symbol"] = active_symbol
                orders = await broker.fetch_open_orders(**request)
        except TypeError:
            active_symbol = str(terminal._current_chart_symbol() or "").strip()
            if active_symbol:
                orders = await broker.fetch_open_orders(active_symbol, 200)
            else:
                orders = await broker.fetch_open_orders()
        except Exception as exc:
            terminal.logger.debug("Open orders refresh failed: %s", exc)

    terminal._latest_open_orders_snapshot = orders or []
    terminal._populate_open_orders_table(terminal._latest_open_orders_snapshot)


def schedule_open_orders_refresh(terminal):
    task = getattr(terminal, "_open_orders_refresh_task", None)
    if task is not None and not task.done():
        return

    try:
        terminal._open_orders_refresh_task = asyncio.get_event_loop().create_task(terminal._refresh_open_orders_async())
    except Exception as exc:
        terminal.logger.debug("Unable to schedule open-orders refresh: %s", exc)


async def load_persisted_runtime_data(terminal):
    loader = getattr(terminal.controller, "_load_recent_trades", None)
    if loader is None:
        return

    try:
        trades = await loader(limit=min(int(terminal.MAX_LOG_ROWS or 200), 200))
    except Exception:
        terminal.logger.exception("Failed to load persisted trade history")
        return

    for trade in trades:
        terminal._update_trade_log(trade)
