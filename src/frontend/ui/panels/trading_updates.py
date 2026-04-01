from PySide6.QtGui import QColor
from PySide6.QtWidgets import QTableWidgetItem


def _filter_query(terminal, attr_name):
    widget = getattr(terminal, attr_name, None)
    if widget is None:
        return ""
    try:
        return str(widget.text() or "").strip().lower()
    except Exception:
        return ""


def _row_matches_query(table, row, query):
    if not query:
        return True
    fragments = []
    for column in range(table.columnCount()):
        item = table.item(row, column)
        if item is None:
            continue
        text = str(item.text() or "").strip()
        if text:
            fragments.append(text.lower())
        tooltip = str(item.toolTip() or "").strip()
        if tooltip:
            fragments.append(tooltip.lower())
    haystack = " ".join(fragments)
    return query in haystack


def _set_filter_summary(terminal, attr_name, *, visible, total, empty_label, noun):
    label = getattr(terminal, attr_name, None)
    if label is None:
        return
    if total <= 0:
        label.setText(empty_label)
        return
    if visible >= total:
        label.setText(f"Showing all {noun}")
        return
    label.setText(f"Showing {visible} of {total} {noun}")


def normalize_position_entry(terminal, raw):
    if raw is None:
        return None

    if isinstance(raw, dict):
        symbol = raw.get("symbol", "")
        side = raw.get("side", "")
        amount = raw.get("amount", raw.get("size", raw.get("quantity", raw.get("qty", 0))))
        entry = raw.get("entry_price", raw.get("avg_entry_price", raw.get("price", raw.get("avg_price", 0))))
        mark = raw.get("mark_price", raw.get("market_price"))
        pnl = raw.get("pnl", raw.get("unrealized_pnl", raw.get("unrealized_pl", raw.get("pl"))))
        realized_pnl = raw.get("realized_pnl", raw.get("realized_pl"))
        financing = raw.get("financing")
        margin_used = raw.get("margin_used", raw.get("marginUsed"))
        resettable_pl = raw.get("resettable_pl", raw.get("resettablePL"))
        units = raw.get("units")
        position_id = raw.get("position_id", raw.get("id", raw.get("trade_id")))
        position_key = raw.get("position_key", raw.get("key", position_id))
        position_side = raw.get("position_side", side)
    else:
        symbol = getattr(raw, "symbol", "")
        side = getattr(raw, "side", "")
        amount = getattr(raw, "amount", getattr(raw, "size", getattr(raw, "quantity", getattr(raw, "qty", 0))))
        entry = getattr(raw, "entry_price", getattr(raw, "avg_entry_price", getattr(raw, "avg_price", getattr(raw, "price", 0))))
        mark = getattr(raw, "mark_price", getattr(raw, "market_price", None))
        pnl = getattr(raw, "pnl", getattr(raw, "unrealized_pnl", getattr(raw, "unrealized_pl", None)))
        realized_pnl = getattr(raw, "realized_pnl", getattr(raw, "realized_pl", None))
        financing = getattr(raw, "financing", None)
        margin_used = getattr(raw, "margin_used", getattr(raw, "marginUsed", None))
        resettable_pl = getattr(raw, "resettable_pl", getattr(raw, "resettablePL", None))
        units = getattr(raw, "units", None)
        position_id = getattr(raw, "position_id", getattr(raw, "id", getattr(raw, "trade_id", None)))
        position_key = getattr(raw, "position_key", getattr(raw, "key", position_id))
        position_side = getattr(raw, "position_side", side)

    try:
        amount = float(amount or 0)
    except Exception:
        amount = 0.0
    try:
        entry = float(entry or 0)
    except Exception:
        entry = 0.0
    try:
        mark = float(mark) if mark not in (None, "") else None
    except Exception:
        mark = None
    try:
        pnl = float(pnl) if pnl not in (None, "") else None
    except Exception:
        pnl = None
    try:
        realized_pnl = float(realized_pnl) if realized_pnl not in (None, "") else None
    except Exception:
        realized_pnl = None
    try:
        financing = float(financing) if financing not in (None, "") else None
    except Exception:
        financing = None
    try:
        margin_used = float(margin_used) if margin_used not in (None, "") else None
    except Exception:
        margin_used = None
    try:
        resettable_pl = float(resettable_pl) if resettable_pl not in (None, "") else None
    except Exception:
        resettable_pl = None
    try:
        units = float(units) if units not in (None, "") else None
    except Exception:
        units = None

    normalized_symbol = str(symbol or "")
    if not normalized_symbol:
        return None

    normalized_side = str(side or "").lower()
    if not normalized_side:
        normalized_side = "long" if amount >= 0 else "short"
    abs_amount = abs(amount)

    if mark is None or mark <= 0:
        mark = terminal._lookup_symbol_mid_price(normalized_symbol)

    value = abs_amount * float(mark or entry or 0)
    if pnl is None and mark is not None and entry:
        direction = 1.0 if normalized_side != "short" else -1.0
        pnl = (float(mark) - entry) * abs_amount * direction

    return {
        "symbol": normalized_symbol,
        "side": normalized_side,
        "position_side": str(position_side or normalized_side).lower(),
        "position_id": str(position_id or "").strip(),
        "position_key": str(position_key or position_id or "").strip(),
        "amount": abs_amount,
        "units": float(units if units is not None else (abs_amount if normalized_side != "short" else -abs_amount)),
        "entry_price": entry,
        "mark_price": float(mark or 0),
        "value": value,
        "pnl": float(pnl or 0),
        "realized_pnl": float(realized_pnl or 0),
        "financing": float(financing or 0),
        "margin_used": float(margin_used or 0),
        "resettable_pl": float(resettable_pl or 0),
    }


def populate_positions_table(terminal, positions):
    table = getattr(terminal, "positions_table", None)
    if table is None:
        return
    close_all_btn = getattr(terminal, "positions_close_all_button", None)
    if table.columnCount() < 8:
        table.setColumnCount(8)
        table.setHorizontalHeaderLabels(
            ["Symbol", "Side", "Amount", "Entry", "Mark", "Value", "P/L", "Action"]
        )

    normalized_positions = []
    for pos in positions or []:
        normalized = terminal._normalize_position_entry(pos)
        if normalized is not None and normalized["amount"] > 0:
            normalized_positions.append(normalized)

    normalized_positions.sort(
        key=lambda item: (
            item["symbol"],
            item["side"],
            item.get("position_id") or item.get("position_key") or "",
        )
    )
    table.setRowCount(len(normalized_positions))
    if close_all_btn is not None:
        close_all_btn.setEnabled(bool(getattr(terminal.controller, "broker", None)) and bool(normalized_positions))

    for row, pos in enumerate(normalized_positions):
        values = [
            pos["symbol"],
            pos["side"].upper(),
            f"{pos['amount']:.6f}".rstrip("0").rstrip("."),
            f"{pos['entry_price']:.6f}".rstrip("0").rstrip("."),
            f"{pos['mark_price']:.6f}".rstrip("0").rstrip("."),
            f"{pos['value']:.2f}",
            f"{pos['pnl']:.2f}",
        ]
        for col, value in enumerate(values):
            item = QTableWidgetItem(value)
            if col == 6:
                item.setForeground(QColor("#32d296" if pos["pnl"] >= 0 else "#ef5350"))
            table.setItem(row, col, item)
        table.setCellWidget(row, 7, terminal._build_position_close_button(pos, compact=True))

    apply_positions_filter(terminal)
    table.resizeColumnsToContents()
    table.horizontalHeader().setStretchLastSection(True)


def normalize_open_order_entry(terminal, order):
    if not isinstance(order, dict):
        return None

    symbol = str(order.get("symbol") or "").strip()
    if not symbol:
        return None

    side = str(order.get("side") or "").strip().lower()
    order_type = str(order.get("type") or order.get("order_type") or "").strip().lower()
    status = str(order.get("status") or "").strip().lower()

    amount = abs(float(order.get("amount") or order.get("qty") or order.get("size") or 0) or 0)
    filled = abs(float(order.get("filled") or order.get("filled_qty") or 0) or 0)
    remaining = max(amount - filled, 0.0)

    try:
        price = float(order.get("price")) if order.get("price") not in (None, "") else None
    except Exception:
        price = None
    if price is not None and price <= 0:
        price = None

    mark = terminal._lookup_symbol_mid_price(symbol)
    if mark is not None and mark <= 0:
        mark = None

    pnl = order.get("pnl")
    if pnl in (None, ""):
        pnl = order.get("unrealized_pnl", order.get("unrealizedPnl"))
    try:
        pnl = float(pnl) if pnl not in (None, "") else None
    except Exception:
        pnl = None

    if pnl is None and price is not None and mark is not None and remaining > 0:
        direction = -1.0 if side == "sell" else 1.0
        pnl = (float(mark) - float(price)) * remaining * direction

    return {
        "symbol": symbol,
        "side": side or "-",
        "type": order_type or "-",
        "price": price,
        "mark": mark,
        "amount": amount,
        "filled": filled,
        "remaining": remaining,
        "status": status or "-",
        "pnl": pnl,
        "order_id": str(order.get("id") or order.get("order_id") or ""),
    }


def populate_open_orders_table(terminal, orders):
    table = getattr(terminal, "open_orders_table", None)
    if table is None:
        return
    if table.columnCount() < 11:
        table.setColumnCount(11)

    normalized_orders = []
    for order in orders or []:
        normalized = terminal._normalize_open_order_entry(order)
        if normalized is not None:
            normalized_orders.append(normalized)

    normalized_orders.sort(key=lambda item: (item["symbol"], item["status"], item["order_id"]))
    table.setRowCount(len(normalized_orders))

    for row, order in enumerate(normalized_orders):
        price_text = "-" if order["price"] is None else f"{order['price']:.6f}".rstrip("0").rstrip(".")
        mark_text = "-" if order["mark"] is None else f"{order['mark']:.6f}".rstrip("0").rstrip(".")
        pnl_value = order["pnl"]
        pnl_text = "-" if pnl_value is None else f"{float(pnl_value):.2f}"

        values = [
            order["symbol"],
            order["side"].upper(),
            order["type"].upper(),
            price_text,
            mark_text,
            f"{order['amount']:.6f}".rstrip("0").rstrip("."),
            f"{order['filled']:.6f}".rstrip("0").rstrip("."),
            f"{order['remaining']:.6f}".rstrip("0").rstrip("."),
            order["status"].replace("_", " ").upper(),
            pnl_text,
            order["order_id"],
        ]

        for col, value in enumerate(values):
            item = QTableWidgetItem(value)
            if col == 8:
                status_value = order["status"]
                if "partial" in status_value:
                    item.setForeground(QColor("#f0a35e"))
                elif status_value in {"open", "pending", "submitted", "accepted", "new"}:
                    item.setForeground(QColor("#65a3ff"))
            elif col == 9 and pnl_value is not None:
                    item.setForeground(QColor("#32d296" if float(pnl_value) >= 0 else "#ef5350"))
            table.setItem(row, col, item)

    apply_open_orders_filter(terminal)
    table.resizeColumnsToContents()
    table.horizontalHeader().setStretchLastSection(True)


def normalize_trade_log_entry(terminal, trade):
    if not isinstance(trade, dict):
        return None

    normalized = {
        "trade_db_id": trade.get("trade_db_id", trade.get("id", "")),
        "timestamp": trade.get("timestamp", ""),
        "symbol": trade.get("symbol", ""),
        "source": terminal._format_trade_source_label(trade.get("source", "bot")),
        "side": trade.get("side", ""),
        "price": trade.get("price", ""),
        "size": trade.get("size", trade.get("amount", "")),
        "order_type": trade.get("order_type", trade.get("type", "")),
        "status": trade.get("status", ""),
        "order_id": trade.get("order_id", trade.get("id", "")),
        "pnl": trade.get("pnl", ""),
        "stop_loss": trade.get("stop_loss", trade.get("sl", "")),
        "take_profit": trade.get("take_profit", trade.get("tp", "")),
        "reason": trade.get("reason", ""),
        "strategy_name": trade.get("strategy_name", ""),
        "confidence": trade.get("confidence", ""),
        "expected_price": trade.get("expected_price", ""),
        "spread_bps": trade.get("spread_bps", ""),
        "slippage_bps": trade.get("slippage_bps", ""),
        "fee": trade.get("fee", ""),
        "setup": trade.get("setup", ""),
        "outcome": trade.get("outcome", ""),
        "lessons": trade.get("lessons", ""),
        "blocked_by_guard": bool(trade.get("blocked_by_guard", False)),
    }
    return normalized


def format_trade_log_value(_terminal, value):
    if value is None:
        return ""
    return str(value)


def format_trade_source_label(_terminal, value):
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    mapping = {
        "chatgpt": "Sopotek Pilot",
        "manual": "Manual",
        "bot": "Bot",
        "chart_double_click": "Chart Double Click",
        "chart_context_menu": "Chart Context Menu",
    }
    return mapping.get(normalized, str(value).replace("_", " ").title())


def trade_log_row_for_entry(terminal, entry):
    order_id = str(entry.get("order_id") or "").strip()
    if not order_id:
        return None

    for row in range(terminal.trade_log.rowCount()):
        item = terminal.trade_log.item(row, 8)
        if item is not None and item.text().strip() == order_id:
            return row
    return None


def update_trade_log(terminal, trade):
    entry = terminal._normalize_trade_log_entry(trade)
    if entry is None:
        return
    if terminal.trade_log.columnCount() < 10:
        terminal.trade_log.setColumnCount(10)

    row = terminal._trade_log_row_for_entry(entry)
    if row is None:
        row = terminal.trade_log.rowCount()

    if row == terminal.trade_log.rowCount() and row >= terminal.MAX_LOG_ROWS:
        terminal.trade_log.removeRow(0)
        row = terminal.trade_log.rowCount()

    if row == terminal.trade_log.rowCount():
        terminal.trade_log.insertRow(row)

    column_values = [
        entry["timestamp"],
        entry["symbol"],
        entry["source"],
        entry["side"],
        entry["price"],
        entry["size"],
        entry["order_type"],
        entry["status"],
        entry["order_id"],
        entry["pnl"],
    ]
    for column, value in enumerate(column_values):
        terminal.trade_log.setItem(row, column, QTableWidgetItem(terminal._format_trade_log_value(value)))

    tooltip_parts = []
    if entry.get("stop_loss") not in ("", None):
        tooltip_parts.append(f"SL: {entry.get('stop_loss')}")
    if entry.get("take_profit") not in ("", None):
        tooltip_parts.append(f"TP: {entry.get('take_profit')}")
    if entry.get("reason") not in ("", None):
        prefix = "Guard" if entry.get("blocked_by_guard") else "Reason"
        tooltip_parts.append(f"{prefix}: {entry.get('reason')}")
    if entry.get("strategy_name") not in ("", None):
        tooltip_parts.append(f"Strategy: {entry.get('strategy_name')}")
    if entry.get("confidence") not in ("", None, 0):
        tooltip_parts.append(f"Confidence: {entry.get('confidence')}")
    if entry.get("spread_bps") not in ("", None):
        tooltip_parts.append(f"Spread: {entry.get('spread_bps')} bps")
    if entry.get("slippage_bps") not in ("", None):
        tooltip_parts.append(f"Slippage: {entry.get('slippage_bps')} bps")
    if entry.get("fee") not in ("", None):
        tooltip_parts.append(f"Fee: {entry.get('fee')}")
    if tooltip_parts:
        tooltip = " | ".join(tooltip_parts)
        for column in range(terminal.trade_log.columnCount()):
            item = terminal.trade_log.item(row, column)
            if item is not None:
                item.setToolTip(tooltip)

    apply_trade_log_filter(terminal)
    terminal.trade_log.horizontalHeader().setStretchLastSection(True)
    terminal._refresh_performance_views()


def apply_positions_filter(terminal):
    table = getattr(terminal, "positions_table", None)
    if table is None:
        return
    query = _filter_query(terminal, "positions_filter_input")
    visible_rows = 0
    total_rows = table.rowCount()
    for row in range(total_rows):
        matches = _row_matches_query(table, row, query)
        table.setRowHidden(row, not matches)
        if matches:
            visible_rows += 1
    _set_filter_summary(
        terminal,
        "positions_filter_summary",
        visible=visible_rows,
        total=total_rows,
        empty_label="Showing all positions",
        noun="positions",
    )


def apply_open_orders_filter(terminal):
    table = getattr(terminal, "open_orders_table", None)
    if table is None:
        return
    query = _filter_query(terminal, "open_orders_filter_input")
    visible_rows = 0
    total_rows = table.rowCount()
    for row in range(total_rows):
        matches = _row_matches_query(table, row, query)
        table.setRowHidden(row, not matches)
        if matches:
            visible_rows += 1
    _set_filter_summary(
        terminal,
        "open_orders_filter_summary",
        visible=visible_rows,
        total=total_rows,
        empty_label="Showing all open orders",
        noun="open orders",
    )


def apply_trade_log_filter(terminal):
    table = getattr(terminal, "trade_log", None)
    if table is None:
        return
    query = _filter_query(terminal, "trade_log_filter_input")
    visible_rows = 0
    total_rows = table.rowCount()
    for row in range(total_rows):
        matches = _row_matches_query(table, row, query)
        table.setRowHidden(row, not matches)
        if matches:
            visible_rows += 1
    _set_filter_summary(
        terminal,
        "trade_log_filter_summary",
        visible=visible_rows,
        total=total_rows,
        empty_label="Showing all trade log rows",
        noun="trade log rows",
    )
