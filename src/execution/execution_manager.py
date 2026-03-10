import asyncio
import logging
import time
from datetime import datetime, timezone

from event_bus.event import Event
from event_bus.event_types import EventType


class ExecutionManager:
    TERMINAL_ORDER_STATUSES = {
        "filled",
        "closed",
        "canceled",
        "cancelled",
        "rejected",
        "expired",
        "failed",
    }
    FILLED_ORDER_STATUSES = {"filled", "closed"}

    def __init__(self, broker, event_bus, router, trade_repository=None, trade_notifier=None):

        self.broker = broker
        self.bus = event_bus
        self.router = router
        self.trade_repository = trade_repository
        self.trade_notifier = trade_notifier
        self.logger = logging.getLogger("ExecutionManager")

        self.running = False
        self._symbol_cooldowns = {}
        self._execution_lock = asyncio.Lock()
        self._balance_buffer = 0.98
        self._order_tracking_tasks = {}
        self._tracked_orders = {}
        self._order_tracking_interval = 2.0
        self._order_tracking_timeout = 900.0

        # Subscribe to ORDER events
        self.bus.subscribe(EventType.ORDER, self.on_order)

    async def start(self):

        self.running = True

    async def stop(self):

        self.running = False
        for task in list(self._order_tracking_tasks.values()):
            if task is not None and not task.done():
                task.cancel()
        self._order_tracking_tasks.clear()
        self._tracked_orders.clear()

    async def on_order(self, event):

        if not self.running:
            return

        try:
            await self.execute(event.data)

        except Exception as e:

            print("Execution error:", e)

    def _cooldown_remaining(self, symbol):
        expires_at = self._symbol_cooldowns.get(symbol)
        if expires_at is None:
            return 0.0

        remaining = expires_at - time.monotonic()
        if remaining <= 0:
            self._symbol_cooldowns.pop(symbol, None)
            return 0.0

        return remaining

    def _set_cooldown(self, symbol, seconds, reason):
        self._symbol_cooldowns[symbol] = time.monotonic() + seconds
        self.logger.warning(
            "Skipping %s for %.0fs: %s",
            symbol,
            seconds,
            reason,
        )

    async def _fetch_reference_price(self, symbol, side, requested_price=None):
        if requested_price is not None:
            return float(requested_price)

        if not hasattr(self.broker, "fetch_ticker"):
            return None

        try:
            ticker = await self.broker.fetch_ticker(symbol)
        except Exception as exc:
            self.logger.debug("Reference price fetch failed for %s: %s", symbol, exc)
            return None
        if not isinstance(ticker, dict):
            return None

        if str(side).lower() == "buy":
            candidates = ("ask", "askPrice", "price", "last", "close")
        else:
            candidates = ("bid", "bidPrice", "price", "last", "close")

        for key in candidates:
            value = ticker.get(key)
            if value is None:
                continue
            try:
                price = float(value)
            except (TypeError, ValueError):
                continue
            if price > 0:
                return price

        return None

    def _extract_free_balances(self, balance):
        if not isinstance(balance, dict):
            return {}

        if isinstance(balance.get("free"), dict):
            return balance["free"]

        skip = {"free", "used", "total", "info", "raw", "equity", "cash", "currency"}
        return {k: v for k, v in balance.items() if k not in skip}

    def _get_market(self, symbol):
        exchange = getattr(self.broker, "exchange", None)
        markets = getattr(exchange, "markets", None)
        if isinstance(markets, dict):
            return markets.get(symbol)
        return None

    def _apply_amount_precision(self, symbol, amount):
        exchange = getattr(self.broker, "exchange", None)
        if exchange and hasattr(exchange, "amount_to_precision"):
            try:
                return float(exchange.amount_to_precision(symbol, amount))
            except Exception:
                pass

        return float(amount)

    def _normalize_order_status(self, status):
        normalized = str(status or "").strip().lower().replace("-", "_").replace(" ", "_")
        mapping = {
            "cancelled": "canceled",
            "partiallyfilled": "partially_filled",
            "partial_fill": "partially_filled",
            "partial_filled": "partially_filled",
            "pending_new": "open",
            "accepted_for_bidding": "open",
            "done_for_day": "expired",
        }
        return mapping.get(normalized, normalized or "unknown")

    def _is_terminal_order_status(self, status):
        return self._normalize_order_status(status) in self.TERMINAL_ORDER_STATUSES

    def _safe_float(self, value, default=0.0):
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    def _extract_order_amount(self, execution, fallback_amount=0.0):
        if not isinstance(execution, dict):
            return abs(self._safe_float(fallback_amount, 0.0))

        for key in ("amount", "qty", "quantity", "size", "filled_qty", "filled"):
            value = execution.get(key)
            if value is None:
                continue
            amount = abs(self._safe_float(value, 0.0))
            if amount > 0:
                return amount

        return abs(self._safe_float(fallback_amount, 0.0))

    def _extract_filled_amount(self, execution, fallback_amount=0.0, status=None):
        if not isinstance(execution, dict):
            return 0.0

        for key in ("filled", "filled_qty", "filled_amount", "executed_qty", "executedQty"):
            value = execution.get(key)
            if value is None:
                continue
            filled = abs(self._safe_float(value, 0.0))
            if filled > 0:
                return filled

        normalized_status = self._normalize_order_status(status or execution.get("status"))
        if normalized_status in self.FILLED_ORDER_STATUSES:
            return self._extract_order_amount(execution, fallback_amount=fallback_amount)

        return 0.0

    def _extract_order_price(self, execution, fallback_price=None):
        if not isinstance(execution, dict):
            return fallback_price

        for key in ("average", "average_price", "avgPrice", "filled_avg_price", "price"):
            value = execution.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue

        return fallback_price

    def _build_trade_payload(self, execution, submitted_order):
        execution = execution or {}
        submitted_order = submitted_order or {}

        status = self._normalize_order_status(execution.get("status") or submitted_order.get("status"))
        timestamp = (
            execution.get("timestamp")
            or submitted_order.get("timestamp")
            or datetime.now(timezone.utc).isoformat()
        )

        return {
            "symbol": execution.get("symbol") or submitted_order.get("symbol"),
            "side": str(execution.get("side") or submitted_order.get("side") or "").upper(),
            "price": self._extract_order_price(execution, fallback_price=submitted_order.get("price")),
            "size": self._extract_order_amount(execution, fallback_amount=submitted_order.get("amount", 0.0)),
            "filled_size": self._extract_filled_amount(
                execution,
                fallback_amount=submitted_order.get("amount", 0.0),
                status=status,
            ),
            "order_type": execution.get("type") or submitted_order.get("type"),
            "status": status,
            "order_id": execution.get("id") or submitted_order.get("id"),
            "timestamp": timestamp,
            "stop_loss": execution.get("stop_loss", submitted_order.get("stop_loss")),
            "take_profit": execution.get("take_profit", submitted_order.get("take_profit")),
            "pnl": execution.get("pnl", submitted_order.get("pnl")),
        }

    def _payload_fingerprint(self, payload):
        return (
            payload.get("status"),
            payload.get("price"),
            payload.get("size"),
            payload.get("filled_size"),
            payload.get("pnl"),
            payload.get("timestamp"),
        )

    async def _persist_trade_update(self, payload):
        if self.trade_repository is not None:
            try:
                await asyncio.to_thread(
                    getattr(self.trade_repository, "save_or_update_trade", self.trade_repository.save_trade),
                    payload.get("symbol"),
                    payload.get("side"),
                    payload.get("size", 0.0),
                    payload.get("price") if payload.get("price") is not None else 0.0,
                    getattr(self.broker, "exchange_name", None),
                    payload.get("order_id"),
                    payload.get("order_type"),
                    payload.get("status"),
                    payload.get("timestamp"),
                )
            except Exception as exc:
                self.logger.debug("Trade persistence failed for %s: %s", payload.get("symbol"), exc)

        if callable(self.trade_notifier):
            try:
                self.trade_notifier(dict(payload))
            except Exception as exc:
                self.logger.debug("Trade notification failed for %s: %s", payload.get("symbol"), exc)

    async def _publish_fill_delta(self, payload, tracker_state):
        filled_size = max(self._safe_float(payload.get("filled_size"), 0.0), 0.0)
        previous_filled = max(self._safe_float(tracker_state.get("filled_size"), 0.0), 0.0)
        delta = filled_size - previous_filled
        if delta <= 0:
            return

        await self.bus.publish(
            Event(
                EventType.FILL,
                {
                    "symbol": payload.get("symbol"),
                    "side": payload.get("side"),
                    "qty": delta,
                    "price": payload.get("price"),
                },
            )
        )

    async def _handle_order_update(self, execution, submitted_order, allow_tracking=True):
        payload = self._build_trade_payload(execution, submitted_order)
        order_id = str(payload.get("order_id") or "").strip()
        tracker_state = self._tracked_orders.get(order_id, {}) if order_id else {}

        await self._publish_fill_delta(payload, tracker_state)

        fingerprint = self._payload_fingerprint(payload)
        if fingerprint != tracker_state.get("fingerprint"):
            await self._persist_trade_update(payload)

        if order_id:
            self._tracked_orders[order_id] = {
                "fingerprint": fingerprint,
                "filled_size": payload.get("filled_size", 0.0),
                "status": payload.get("status"),
                "symbol": payload.get("symbol"),
            }

        if order_id and allow_tracking and not self._is_terminal_order_status(payload.get("status")):
            self._ensure_order_tracking(order_id, payload.get("symbol"), dict(submitted_order or {}))

        if order_id and self._is_terminal_order_status(payload.get("status")):
            task = self._order_tracking_tasks.pop(order_id, None)
            if task is not None and not task.done():
                task.cancel()
            self._tracked_orders.pop(order_id, None)

        return payload

    def _ensure_order_tracking(self, order_id, symbol, submitted_order):
        if not order_id or not hasattr(self.broker, "fetch_order"):
            return

        task = self._order_tracking_tasks.get(order_id)
        if task is not None and not task.done():
            return

        self._order_tracking_tasks[order_id] = asyncio.create_task(
            self._track_order_until_terminal(order_id, symbol, submitted_order)
        )

    async def _track_order_until_terminal(self, order_id, symbol, submitted_order):
        started_at = time.monotonic()
        try:
            while time.monotonic() - started_at <= self._order_tracking_timeout:
                await asyncio.sleep(self._order_tracking_interval)

                try:
                    snapshot = await self.broker.fetch_order(order_id, symbol=symbol)
                except TypeError:
                    snapshot = await self.broker.fetch_order(order_id)
                except NotImplementedError:
                    self.logger.debug("Broker does not support fetch_order tracking for %s", order_id)
                    return
                except Exception as exc:
                    self.logger.debug("Order status refresh failed for %s: %s", order_id, exc)
                    continue

                if not isinstance(snapshot, dict):
                    continue

                payload = await self._handle_order_update(snapshot, submitted_order, allow_tracking=False)
                if self._is_terminal_order_status(payload.get("status")):
                    return
        except asyncio.CancelledError:
            raise
        finally:
            self._order_tracking_tasks.pop(order_id, None)

    async def _prepare_order(self, order):
        symbol = order["symbol"]
        side = order["side"]

        if self._cooldown_remaining(symbol) > 0:
            return None

        market = self._get_market(symbol)
        if market is not None and market.get("active") is False:
            self._set_cooldown(symbol, 300, "market is inactive")
            return None

        price = await self._fetch_reference_price(symbol, side, order.get("price"))

        amount = float(order["amount"])
        base_currency, quote_currency = (symbol.split("/", 1) + [None])[:2]

        balance = {}
        if hasattr(self.broker, "fetch_balance"):
            try:
                balance = self._extract_free_balances(await self.broker.fetch_balance())
            except Exception as exc:
                self.logger.debug("Balance fetch failed for %s: %s", symbol, exc)

        available_quote = None
        available_base = None
        if quote_currency:
            available_quote = float(balance.get(quote_currency, 0) or 0)
        if base_currency:
            available_base = float(balance.get(base_currency, 0) or 0)

        if side == "buy" and price and available_quote is not None:
            spendable_quote = available_quote * self._balance_buffer
            if spendable_quote <= 0:
                self._set_cooldown(symbol, 120, f"no available {quote_currency} balance")
                return None
            affordable_amount = spendable_quote / price
            amount = min(amount, affordable_amount)

        if side == "sell" and available_base is not None:
            liquid_base = available_base * self._balance_buffer
            if liquid_base <= 0:
                self._set_cooldown(symbol, 120, f"no available {base_currency} balance")
                return None
            amount = min(amount, liquid_base)

        limits = market.get("limits", {}) if isinstance(market, dict) else {}
        min_amount = ((limits.get("amount") or {}).get("min"))
        min_cost = ((limits.get("cost") or {}).get("min"))

        if price and min_cost:
            min_cost_amount = float(min_cost) / price
            amount = max(amount, min_cost_amount)

        if min_amount:
            amount = max(amount, float(min_amount))

        amount = self._apply_amount_precision(symbol, amount)

        if amount <= 0:
            self._set_cooldown(symbol, 120, "computed order amount is zero")
            return None

        if (
            side == "buy"
            and price
            and available_quote is not None
            and amount * price > (available_quote * self._balance_buffer) + 1e-12
        ):
            self._set_cooldown(symbol, 120, f"insufficient {quote_currency} balance")
            return None

        if (
            side == "sell"
            and available_base is not None
            and amount > (available_base * self._balance_buffer) + 1e-12
        ):
            self._set_cooldown(symbol, 120, f"insufficient {base_currency} balance")
            return None

        prepared = dict(order)
        prepared["amount"] = amount
        if order.get("price") is not None:
            prepared["price"] = order["price"]

        return prepared


    async def execute(self, signal=None, **kwargs):
        if signal is None:
            signal = {}
        elif not isinstance(signal, dict):
            raise TypeError("signal must be a dict when provided")

        order = {**signal, **kwargs}

        symbol = order.get("symbol")
        side = order.get("side") or order.get("signal")
        amount = order.get("amount")
        if amount is None:
            amount = order.get("size")
        price = order.get("price")
        order_type = order.get("type", "market")
        stop_loss = order.get("stop_loss")
        take_profit = order.get("take_profit")
        params = dict(order.get("params") or {})

        if not symbol:
            raise ValueError("Order symbol is required")
        if not side:
            raise ValueError("Order side is required")
        if amount is None:
            raise ValueError("Order amount is required")

        normalized_order = {
            "symbol": symbol,
            "side": str(side).lower(),
            "amount": amount,
            "type": order_type,
        }

        if price is not None:
            normalized_order["price"] = price
        if stop_loss is not None:
            normalized_order["stop_loss"] = stop_loss
        if take_profit is not None:
            normalized_order["take_profit"] = take_profit
        if params:
            normalized_order["params"] = params

        async with self._execution_lock:
            prepared_order = await self._prepare_order(normalized_order)
            if prepared_order is None:
                return None

            try:
                execution = await self.router.route(prepared_order)
            except Exception as exc:
                message = str(exc)
                lowered = message.lower()
                if any(
                    token in lowered
                    for token in ("market is closed", "min_notional", "insufficient balance", "too many requests", "429")
                ):
                    self._set_cooldown(symbol, 300, message)
                    return None
                raise
            prepared_order["timestamp"] = datetime.now(timezone.utc).isoformat()
            await self._handle_order_update(execution, prepared_order)

        return execution
