import asyncio
import logging
from datetime import datetime, timezone

from manager.portfolio_manager import PortfolioManager
from execution.execution_manager import ExecutionManager
from strategy.strategy_registry import StrategyRegistry
from engines.risk_engine import RiskEngine
from execution.order_router import OrderRouter
from event_bus.event_bus import EventBus
from core.multi_symbol_orchestrator import MultiSymbolOrchestrator
from quant.data_hub import QuantDataHub
from quant.portfolio_allocator import PortfolioAllocator
from quant.portfolio_risk_engine import PortfolioRiskEngine
from quant.signal_engine import SignalEngine
from risk.trader_behavior_guard import TraderBehaviorGuard


class SopotekTrading:

    def __init__(self, controller=None):

        self.controller = controller
        self.logger = logging.getLogger(__name__)

        # =========================
        # BROKER
        # =========================

        self.broker = getattr(controller, "broker", None)

        if self.broker is None:
            raise RuntimeError("Broker not initialized")

        required_methods = ("fetch_ohlcv", "fetch_balance", "create_order")
        missing = [name for name in required_methods if not hasattr(self.broker, name)]
        if missing:
            raise RuntimeError(
                "Controller broker is missing required capabilities: " + ", ".join(missing)
            )

        self.symbols = getattr(controller, "symbols", ["BTC/USDT", "ETH/USDT"])

        # =========================
        # CORE COMPONENTS
        # =========================

        self.strategy = StrategyRegistry()
        self._apply_strategy_preferences()
        self.data_hub = QuantDataHub(
            controller=self.controller,
            market_data_repository=getattr(controller, "market_data_repository", None),
            broker=self.broker,
        )
        self.signal_engine = SignalEngine(self.strategy)

        self.event_bus = EventBus()

        self.portfolio = PortfolioManager(event_bus=self.event_bus)

        self.router = OrderRouter(broker=self.broker)
        self.behavior_guard = TraderBehaviorGuard(
            max_orders_per_hour=24,
            max_orders_per_day=120,
            max_consecutive_losses=4,
            cooldown_after_loss_seconds=900,
            same_symbol_reentry_cooldown_seconds=300,
            max_size_jump_ratio=3.0,
            daily_drawdown_limit_pct=0.06,
        )
        if self.controller is not None:
            self.controller.behavior_guard = self.behavior_guard

        self.execution_manager = ExecutionManager(
            broker=self.broker,
            event_bus=self.event_bus,
            router=self.router,
            trade_repository=getattr(controller, "trade_repository", None),
            trade_notifier=getattr(controller, "handle_trade_execution", None),
            behavior_guard=self.behavior_guard,
        )

        self.risk_engine = None
        self.portfolio_allocator = None
        self.portfolio_risk_engine = None
        self.orchestrator = None

        # =========================
        # SYSTEM SETTINGS
        # =========================

        self.time_frame = getattr(controller, "time_frame", "1h")
        self.limit = getattr(controller, "limit", 50000)
        self.running = False
        self._pipeline_status = {}

        self.logger.info("Sopotek Trading System initialized")

    def _apply_strategy_preferences(self):
        strategy_name = getattr(self.controller, "strategy_name", None)
        strategy_params = getattr(self.controller, "strategy_params", None)
        self.strategy.configure(strategy_name=strategy_name, params=strategy_params)

    def refresh_strategy_preferences(self):
        self._apply_strategy_preferences()
        if self.portfolio_allocator is not None:
            weight_resolver = getattr(self.controller, "active_strategy_weight_map", None) if self.controller is not None else None
            weights = weight_resolver() if callable(weight_resolver) else {str(getattr(self.controller, "strategy_name", "Trend Following")): 1.0}
            self.portfolio_allocator.configure_strategy_weights(strategy_weights=weights, allocation_model="equal_weight")

    def _safe_numeric_value(self, value, fallback):
        if value in (None, ""):
            return float(fallback)
        if isinstance(value, str):
            cleaned = value.strip().replace(",", "")
            if not cleaned:
                return float(fallback)
            value = cleaned
        try:
            return float(value)
        except Exception:
            return float(fallback)

    def _resolve_starting_equity(self, balance=None):
        default_equity = self._safe_numeric_value(
            getattr(self.controller, "initial_capital", 10000),
            10000,
        )
        if not isinstance(balance, dict):
            return default_equity

        total = balance.get("total")
        if isinstance(total, dict):
            for currency in ("USDT", "USD", "USDC", "BUSD"):
                value = total.get(currency)
                numeric = self._safe_numeric_value(value, 0.0)
                if numeric > 0:
                    return numeric
            for value in total.values():
                numeric = self._safe_numeric_value(value, 0.0)
                if numeric > 0:
                    return numeric
        return default_equity

    def _assigned_strategies_for_symbol(self, symbol):
        resolver = getattr(self.controller, "assigned_strategies_for_symbol", None) if self.controller is not None else None
        if callable(resolver):
            try:
                assigned = list(resolver(symbol) or [])
            except Exception:
                assigned = []
            if assigned:
                return assigned
        fallback_name = str(getattr(self.controller, "strategy_name", None) or "Trend Following").strip() or "Trend Following"
        return [{"strategy_name": fallback_name, "score": 1.0, "weight": 1.0, "rank": 1}]

    def _select_strategy_signal(self, normalized_symbol, candles, dataset):
        assigned = self._assigned_strategies_for_symbol(normalized_symbol)
        candidates = []
        for assignment in assigned:
            strategy_name = str(assignment.get("strategy_name") or "").strip()
            if not strategy_name:
                continue
            signal = self.signal_engine.generate_signal(
                candles=candles,
                dataset=dataset,
                strategy_name=strategy_name,
                symbol=normalized_symbol,
            )
            if not signal:
                continue
            weighted_confidence = float(signal.get("confidence", 0.0) or 0.0) * max(0.0001, float(assignment.get("weight", 0.0) or 0.0))
            enriched = dict(signal)
            enriched["strategy_name"] = strategy_name
            enriched["strategy_assignment_weight"] = float(assignment.get("weight", 0.0) or 0.0)
            enriched["strategy_assignment_score"] = float(assignment.get("score", 0.0) or 0.0)
            enriched["strategy_assignment_rank"] = int(assignment.get("rank", 0) or 0)
            candidates.append((weighted_confidence, float(assignment.get("score", 0.0) or 0.0), enriched))
        if not candidates:
            return None, assigned
        candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return candidates[0][2], assigned

    def _resolve_execution_strategy(self, symbol, side, amount, price, signal):
        requested = str(signal.get("execution_strategy") or "").strip().lower()
        if requested:
            return requested

        order_type = str(signal.get("type") or "market").strip().lower()
        portfolio_equity = None
        try:
            portfolio_equity = self.portfolio.equity()
        except Exception:
            portfolio_equity = None
        equity = float(portfolio_equity or getattr(self.risk_engine, "account_equity", 0.0) or 0.0)
        notional = abs(float(amount or 0.0) * float(price or 0.0))
        if equity <= 0 or notional <= 0:
            return order_type

        notional_pct = notional / equity
        if order_type in {"limit", "stop_limit"} and notional_pct >= 0.08:
            return "iceberg"
        if order_type == "market" and notional_pct >= 0.05:
            return "twap"
        return order_type

    def _record_pipeline_status(self, symbol, stage, status, detail=None, signal=None):
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            return

        snapshot = {
            "symbol": normalized_symbol,
            "stage": str(stage or "").strip() or "unknown",
            "status": str(status or "").strip() or "unknown",
            "detail": str(detail or "").strip(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if isinstance(signal, dict):
            snapshot["strategy_name"] = signal.get("strategy_name") or getattr(self.controller, "strategy_name", None)
            snapshot["side"] = signal.get("side")
            snapshot["confidence"] = signal.get("confidence")
        self._pipeline_status[normalized_symbol] = snapshot

    def pipeline_status_snapshot(self):
        return {
            symbol: dict(payload)
            for symbol, payload in (self._pipeline_status or {}).items()
        }

    def _symbols_match(self, left, right):
        normalize = lambda value: str(value or "").strip().upper().replace("-", "/").replace("_", "/")
        left_text = normalize(left)
        right_text = normalize(right)
        return bool(left_text and right_text and left_text == right_text)

    def _position_side(self, position):
        if hasattr(self.broker, "_position_side"):
            try:
                side = self.broker._position_side(position)
                if side:
                    return str(side).strip().lower()
            except Exception:
                pass
        if isinstance(position, dict):
            side = position.get("side")
            if side is not None:
                return str(side).strip().lower()
            for key in ("amount", "qty", "quantity", "size", "contracts"):
                value = position.get(key)
                try:
                    numeric = float(value)
                except Exception:
                    continue
                if numeric < 0:
                    return "short"
                if numeric > 0:
                    return "long"
        return ""

    def _position_amount(self, position):
        if hasattr(self.broker, "_position_amount"):
            try:
                return float(self.broker._position_amount(position) or 0.0)
            except Exception:
                pass
        if isinstance(position, dict):
            for key in ("amount", "qty", "quantity", "size", "contracts"):
                value = position.get(key)
                try:
                    return abs(float(value))
                except Exception:
                    continue
        return 0.0

    def _hedging_mode_active(self):
        resolver = getattr(self.controller, "hedging_is_active", None) if self.controller is not None else None
        if callable(resolver):
            try:
                return bool(resolver(self.broker))
            except Exception:
                return False
        return bool(getattr(self.controller, "hedging_enabled", False)) and bool(getattr(self.broker, "hedging_supported", False))

    def _signal_requests_position_reduction(self, signal):
        signal = signal if isinstance(signal, dict) else {}
        action = str(signal.get("action") or signal.get("intent") or "").strip().lower()
        if action in {"exit", "close", "flatten", "reduce"}:
            return True
        reason = str(signal.get("reason") or "").strip().lower()
        return any(token in reason for token in (" close", "flatten", "reduce", "take profit", "stop out"))

    def _execution_params_for_signal(self, signal):
        params = dict((signal or {}).get("params") or {})
        if not self._hedging_mode_active():
            return params
        params.setdefault(
            "positionFill",
            "REDUCE_ONLY" if self._signal_requests_position_reduction(signal) else "OPEN_ONLY",
        )
        return params

    def _is_exit_like_signal(self, signal_side, position_side, signal):
        normalized_signal = str(signal_side or "").strip().lower()
        normalized_position = str(position_side or "").strip().lower()
        if not normalized_signal or not normalized_position:
            return False

        if normalized_position in {"long", "buy"} and normalized_signal == "sell":
            return True
        if normalized_position in {"short", "sell"} and normalized_signal == "buy":
            return True

        reason = str((signal or {}).get("reason") or "").strip().lower()
        return any(token in reason for token in ("exit", "close", "flatten", "reduce"))

    async def _fetch_symbol_positions(self, symbol):
        if not hasattr(self.broker, "fetch_positions"):
            return []
        try:
            positions = await self.broker.fetch_positions(symbols=[symbol])
        except TypeError:
            positions = await self.broker.fetch_positions()
        except Exception:
            return []
        return [
            position
            for position in (positions or [])
            if isinstance(position, dict)
            and self._symbols_match(position.get("symbol"), symbol)
            and self._position_amount(position) > 0
        ]

    async def _fetch_symbol_open_orders(self, symbol, limit=100):
        if not hasattr(self.broker, "fetch_open_orders"):
            return []
        snapshot = getattr(self.broker, "fetch_open_orders_snapshot", None)
        try:
            if callable(snapshot):
                orders = await snapshot(symbols=[symbol], limit=limit)
            else:
                orders = await self.broker.fetch_open_orders(symbol=symbol, limit=limit)
        except TypeError:
            try:
                orders = await self.broker.fetch_open_orders(symbol)
            except Exception:
                return []
        except Exception:
            return []

        active_statuses = {"open", "pending", "submitted", "accepted", "new", "partially_filled", "partially-filled"}
        filtered = []
        for order in orders or []:
            if not isinstance(order, dict):
                continue
            if not self._symbols_match(order.get("symbol"), symbol):
                continue
            status = str(order.get("status") or "open").strip().lower()
            if status and status not in active_statuses:
                continue
            filtered.append(order)
        return filtered

    async def _cancel_stale_exit_orders(self, symbol, side, signal):
        if self._hedging_mode_active() and not self._signal_requests_position_reduction(signal):
            return 0, "Hedging mode keeps opposite-side entries open."

        positions = await self._fetch_symbol_positions(symbol)
        if not positions:
            return 0, "No live broker position to clean up."

        has_exit_like_position = any(
            self._is_exit_like_signal(side, self._position_side(position), signal)
            for position in positions
        )
        if not has_exit_like_position:
            return 0, "Signal does not oppose a live broker position."

        open_orders = await self._fetch_symbol_open_orders(symbol)
        if not open_orders:
            return 0, "No stale open orders were found for the symbol."

        canceled = 0
        if hasattr(self.broker, "cancel_all_orders"):
            try:
                await self.broker.cancel_all_orders(symbol=symbol)
                return len(open_orders), f"Canceled {len(open_orders)} stale open order(s) before exit handling."
            except TypeError:
                try:
                    await self.broker.cancel_all_orders(symbol)
                    return len(open_orders), f"Canceled {len(open_orders)} stale open order(s) before exit handling."
                except Exception:
                    pass
            except Exception:
                pass

        if not hasattr(self.broker, "cancel_order"):
            return 0, "Broker does not support cancel_order for exit cleanup."

        for order in open_orders:
            order_id = str(order.get("id") or order.get("order_id") or order.get("clientOrderId") or "").strip()
            if not order_id:
                continue
            try:
                await self.broker.cancel_order(order_id, symbol=symbol)
                canceled += 1
            except TypeError:
                try:
                    await self.broker.cancel_order(order_id)
                    canceled += 1
                except Exception:
                    continue
            except Exception:
                continue

        if canceled:
            return canceled, f"Canceled {canceled} stale open order(s) before exit handling."
        return 0, "Unable to cancel stale open orders before exit handling."

    async def process_symbol(self, symbol, timeframe=None, limit=None, publish_debug=True):
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            raise ValueError("Symbol is required")

        target_timeframe = str(timeframe or self.time_frame or "1h").strip() or "1h"
        target_limit = max(1, int(limit or self.limit or 300))

        dataset = await self.data_hub.get_symbol_dataset(
            symbol=normalized_symbol,
            timeframe=target_timeframe,
            limit=target_limit,
        )
        candles = dataset.to_candles()
        if not candles:
            self._record_pipeline_status(normalized_symbol, "data_hub", "empty", "No candles returned for symbol")
            return None

        signal, assigned_strategies = self._select_strategy_signal(normalized_symbol, candles, dataset)

        features = getattr(dataset, "frame", None)
        default_strategy_name = str(getattr(self.controller, "strategy_name", None) or "Trend Following").strip() or "Trend Following"
        display_strategy_name = signal.get("strategy_name") if isinstance(signal, dict) else None
        if not display_strategy_name:
            display_strategy_name = ", ".join(
                str(item.get("strategy_name") or "").strip()
                for item in assigned_strategies[:3]
                if str(item.get("strategy_name") or "").strip()
            ) or default_strategy_name
        display_signal = signal or {
            "symbol": normalized_symbol,
            "side": "hold",
            "amount": 0.0,
            "confidence": 0.0,
            "reason": "No entry signal on the latest scan.",
            "strategy_name": display_strategy_name,
        }

        if publish_debug and self.controller and hasattr(self.controller, "publish_ai_signal"):
            self.controller.publish_ai_signal(normalized_symbol, display_signal, candles=candles)
        if publish_debug and self.controller and hasattr(self.controller, "publish_strategy_debug"):
            self.controller.publish_strategy_debug(
                normalized_symbol,
                display_signal,
                candles=candles,
                features=features,
            )

        if signal:
            self._record_pipeline_status(normalized_symbol, "signal_engine", "signal", signal.get("reason"), signal=signal)
        else:
            self._record_pipeline_status(normalized_symbol, "signal_engine", "hold", display_signal.get("reason"), signal=display_signal)
            return None

        if self.controller and hasattr(self.controller, "apply_news_bias_to_signal"):
            signal = await self.controller.apply_news_bias_to_signal(normalized_symbol, signal)
            if not signal:
                self._record_pipeline_status(
                    normalized_symbol,
                    "news_bias",
                    "blocked",
                    "Signal was neutralized by news bias controls.",
                    signal=display_signal,
                )
                return None

        result = await self.process_signal(normalized_symbol, signal, dataset=dataset)
        if result is None:
            latest = self._pipeline_status.get(normalized_symbol, {})
            if latest.get("status") in {"rejected", "blocked"}:
                return None
            self._record_pipeline_status(
                normalized_symbol,
                "execution_manager",
                "skipped",
                "Signal did not result in an executable order.",
                signal=signal,
            )
            return None

        execution_status = str(result.get("status") or "submitted").strip().lower() if isinstance(result, dict) else "submitted"
        self._record_pipeline_status(
            normalized_symbol,
            "execution_manager",
            execution_status,
            result.get("reason") if isinstance(result, dict) else "",
            signal=signal,
        )
        return result

    # ==========================================
    # START SYSTEM
    # ==========================================

    async def start(self):
        if self.running:
            self.logger.info("Trading system already running")
            return

        if self.broker is None:
            raise RuntimeError("Broker not initialized")



        balance = getattr(self.controller, "balances", {}) or {}
        equity = self._resolve_starting_equity(balance)



        self.risk_engine = RiskEngine(
            account_equity=equity,
            max_portfolio_risk=getattr(self.controller, "max_portfolio_risk", 100),
            max_risk_per_trade=getattr(self.controller, "max_risk_per_trade", 50),
            max_position_size_pct=getattr(self.controller, "max_position_size_pct", 25),
            max_gross_exposure_pct=getattr(self.controller, "max_gross_exposure_pct", 30),
        )
        active_strategy = getattr(self.controller, "strategy_name", None) or "Trend Following"
        weight_resolver = getattr(self.controller, "active_strategy_weight_map", None) if self.controller is not None else None
        strategy_weights = weight_resolver() if callable(weight_resolver) else {str(active_strategy): 1.0}
        self.portfolio_allocator = PortfolioAllocator(
            account_equity=equity,
            strategy_weights=strategy_weights,
            allocation_model="equal_weight",
            max_strategy_allocation_pct=1.0,
            rebalance_threshold_pct=0.15,
            volatility_target_pct=0.20,
        )
        self.portfolio_risk_engine = PortfolioRiskEngine(
            account_equity=equity,
            max_portfolio_risk=getattr(self.controller, "max_portfolio_risk", 0.10),
            max_risk_per_trade=getattr(self.controller, "max_risk_per_trade", 0.02),
            max_position_size_pct=getattr(self.controller, "max_position_size_pct", 0.10),
            max_gross_exposure_pct=getattr(self.controller, "max_gross_exposure_pct", 2.0),
            max_symbol_exposure_pct=min(
                0.30,
                max(0.05, float(getattr(self.controller, "max_position_size_pct", 0.10) or 0.10) * 1.5),
            ),
        )
        if self.controller is not None:
            self.controller.portfolio_allocator = self.portfolio_allocator
            self.controller.institutional_risk_engine = self.portfolio_risk_engine
        if self.behavior_guard is not None:
            self.behavior_guard.record_equity(equity)

        self.orchestrator = MultiSymbolOrchestrator(controller=self.controller,
            broker=self.broker,
            strategy=self.strategy,
            execution_manager=self.execution_manager,
            risk_engine=self.risk_engine,
            signal_processor=self.process_symbol,
        )


        self.running = True
        self.logger.info(f"Loaded {len(self.symbols)} symbols")
        await self.execution_manager.start()
        await self.orchestrator.start(symbols=self.symbols)

    # ==========================================
    # MAIN TRADING LOOP
    # ==========================================

    async def run(self):

        self.logger.info("Trading loop started")

        while self.running:

            try:
                active_symbols = self.symbols[:100]
                if self.controller and hasattr(self.controller, "get_active_autotrade_symbols"):
                    try:
                        resolved = self.controller.get_active_autotrade_symbols()
                    except Exception:
                        resolved = []
                    if resolved:
                        active_symbols = resolved[:100]

                for symbol in active_symbols:
                    await self.process_symbol(
                        symbol,
                        timeframe=self.time_frame,
                        limit=self.limit,
                        publish_debug=True,
                    )

                await asyncio.sleep(5)

            except Exception:
                self.logger.exception("Trading loop error")

    # ==========================================
    # PROCESS SIGNAL
    # ==========================================

    async def process_signal(self, symbol, signal, dataset=None):

        side = signal["side"]
        price = signal.get("price")
        amount = signal["amount"]
        strategy_name = signal.get("strategy_name") or getattr(self.controller, "strategy_name", "Bot")
        if (price is None or float(price or 0) <= 0) and dataset is not None and not getattr(dataset, "empty", True):
            try:
                price = float(dataset.frame.iloc[-1]["close"])
            except Exception:
                price = None
        if price is None or float(price or 0) <= 0:
            self.logger.warning("Trade rejected because no executable reference price was available for %s", symbol)
            return

        canceled_orders, cleanup_reason = await self._cancel_stale_exit_orders(symbol, side, signal)
        if canceled_orders:
            self.logger.info("%s", cleanup_reason)
            self._record_pipeline_status(symbol, "order_cleanup", "approved", cleanup_reason, signal=signal)

        basic_reason = "Approved"
        if hasattr(self.risk_engine, "adjust_trade"):
            allowed, adjusted_amount, basic_reason = self.risk_engine.adjust_trade(float(price), float(amount))
        else:
            allowed, basic_reason = self.risk_engine.validate_trade(float(price), float(amount))
            adjusted_amount = float(amount)

        if not allowed:
            self.logger.warning("Trade rejected by risk engine: %s", basic_reason)
            self._record_pipeline_status(symbol, "risk_engine", "rejected", basic_reason, signal=signal)
            return
        if adjusted_amount + 1e-12 < float(amount):
            self.logger.info(
                "Risk engine reduced %s order size from %.8f to %.8f: %s",
                symbol,
                float(amount),
                adjusted_amount,
                basic_reason,
            )
        amount = adjusted_amount
        self._record_pipeline_status(symbol, "risk_engine", "approved", basic_reason, signal=signal)

        if self.portfolio_allocator is not None:
            try:
                portfolio_equity = self.portfolio.equity()
            except Exception:
                portfolio_equity = None
            if portfolio_equity:
                self.portfolio_allocator.sync_equity(portfolio_equity)
            allocation = await self.portfolio_allocator.allocate_trade(
                symbol=symbol,
                strategy_name=strategy_name,
                side=side,
                amount=amount,
                price=price,
                portfolio=getattr(self.portfolio, "portfolio", None),
                market_prices=getattr(self.portfolio, "market_prices", {}),
                dataset=dataset,
                confidence=signal.get("confidence"),
                active_strategies=[strategy_name],
            )
            if self.controller is not None:
                self.controller.quant_allocation_snapshot = dict(allocation.metrics or {})
            if not allocation.approved:
                self.logger.warning("Trade rejected by portfolio allocator: %s", allocation.reason)
                self._record_pipeline_status(symbol, "portfolio_allocator", "rejected", allocation.reason, signal=signal)
                return
            amount = allocation.adjusted_amount
            self._record_pipeline_status(symbol, "portfolio_allocator", "approved", allocation.reason, signal=signal)

        if self.portfolio_risk_engine is not None:
            try:
                portfolio_equity = self.portfolio.equity()
            except Exception:
                portfolio_equity = None
            if portfolio_equity:
                self.portfolio_risk_engine.sync_equity(portfolio_equity)
            approval = await self.portfolio_risk_engine.approve_trade(
                symbol=symbol,
                side=side,
                amount=amount,
                price=price,
                portfolio=getattr(self.portfolio, "portfolio", None),
                market_prices=getattr(self.portfolio, "market_prices", {}),
                data_hub=self.data_hub,
                dataset=dataset,
                timeframe=self.time_frame,
                strategy_name=signal.get("strategy_name") or getattr(self.controller, "strategy_name", None),
            )
            if self.controller is not None:
                self.controller.quant_risk_snapshot = dict(approval.metrics or {})
            if not approval.approved:
                self.logger.warning("Trade rejected by institutional risk engine: %s", approval.reason)
                self._record_pipeline_status(symbol, "portfolio_risk_engine", "rejected", approval.reason, signal=signal)
                return
            amount = approval.adjusted_amount
            self._record_pipeline_status(symbol, "portfolio_risk_engine", "approved", approval.reason, signal=signal)

        if self.portfolio_allocator is not None:
            self.portfolio_allocator.register_strategy_symbol(symbol, strategy_name)

        margin_closeout_snapshot = {}
        margin_guard = getattr(self.controller, "margin_closeout_snapshot", None) if self.controller is not None else None
        if callable(margin_guard):
            try:
                margin_closeout_snapshot = dict(margin_guard() or {})
            except Exception:
                margin_closeout_snapshot = {}
        if self.controller is not None and margin_closeout_snapshot:
            merged_risk_snapshot = dict(getattr(self.controller, "quant_risk_snapshot", {}) or {})
            merged_risk_snapshot["margin_closeout_guard"] = margin_closeout_snapshot
            self.controller.quant_risk_snapshot = merged_risk_snapshot
        if margin_closeout_snapshot.get("blocked"):
            reason = str(
                margin_closeout_snapshot.get("reason")
                or "Margin closeout guard blocked the trade."
            ).strip()
            self.logger.warning("Trade rejected by margin closeout guard: %s", reason)
            self._record_pipeline_status(symbol, "margin_closeout_guard", "rejected", reason, signal=signal)
            return

        execution_strategy = self._resolve_execution_strategy(symbol, side, amount, price, signal)
        execution_params = self._execution_params_for_signal(signal)

        order = await self.execution_manager.execute(
            symbol=symbol,
            side=side,
            amount=amount,
            price=price,
            source="bot",
            strategy_name=strategy_name,
            reason=signal.get("reason"),
            confidence=signal.get("confidence"),
            expected_price=signal.get("price"),
            pnl=signal.get("pnl"),
            execution_strategy=execution_strategy,
            type=signal.get("type", "market"),
            params=execution_params,
        )

        return order

    # ==========================================
    # STOP SYSTEM
    # ==========================================

    async def stop(self):

        self.logger.info("Stopping trading system")

        self.running = False

        orchestrator = getattr(self, "orchestrator", None)
        if orchestrator is not None:
            for worker in list(getattr(orchestrator, "workers", []) or []):
                try:
                    worker.running = False
                except Exception:
                    pass
            shutdown = getattr(orchestrator, "shutdown", None)
            if callable(shutdown):
                try:
                    await shutdown()
                except Exception:
                    self.logger.exception("Orchestrator shutdown failed")

        execution_manager = getattr(self, "execution_manager", None)
        if execution_manager is not None:
            try:
                await execution_manager.stop()
            except Exception:
                self.logger.exception("Execution manager stop failed")
