import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from agents.memory import AgentMemory
from agents.orchestrator import AgentOrchestrator
from agents.portfolio_agent import PortfolioAgent
from agents.regime_agent import RegimeAgent
from agents.risk_agent import RiskAgent as TradingRiskAgent
from agents.signal_aggregation_agent import SignalAggregationAgent
from agents.signal_consensus_agent import SignalConsensusAgent
from agents.signal_fanout import run_signal_agents_parallel
from agents.signal_agent import SignalAgent
from agents.execution_agent import ExecutionAgent as TradingExecutionAgent
from agents.event_driven_runtime import EventDrivenAgentRuntime
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
    MAX_RUNTIME_ANALYSIS_BARS = 500
    ADAPTIVE_TRADE_HISTORY_LIMIT = 300
    ADAPTIVE_TRADE_CACHE_TTL_SECONDS = 15.0
    ADAPTIVE_WEIGHT_MIN = 0.75
    ADAPTIVE_WEIGHT_MAX = 1.35

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
        self.agent_decision_repository = None
        self.agent_memory = AgentMemory(max_events=2000)
        self.agent_memory.add_sink(self._persist_agent_memory_event)
        self.signal_agent_slots = max(1, int(getattr(controller, "max_signal_agents", 3) or 3))
        self.signal_agents = []
        for slot_index in range(self.signal_agent_slots):
            self.signal_agents.append(
                SignalAgent(
                    selector=self._select_strategy_signal_for_slot(slot_index),
                    name="SignalAgent" if slot_index == 0 else f"SignalAgent{slot_index + 1}",
                    news_bias_applier=self._apply_news_bias,
                    memory=self.agent_memory,
                    event_bus=self.event_bus,
                    candidate_mode=True,
                )
            )
        self.signal_aggregation_agent = SignalAggregationAgent(
            display_builder=self._build_display_signal,
            publisher=self._publish_signal_context,
            memory=self.agent_memory,
            event_bus=self.event_bus,
        )
        self.signal_consensus_agent = SignalConsensusAgent(
            minimum_votes=max(1, int(getattr(controller, "minimum_signal_votes", 2) or 2)),
            memory=self.agent_memory,
            event_bus=self.event_bus,
        )
        self.signal_agent = self.signal_agents[0]
        self.agent_orchestrator = AgentOrchestrator(
            agents=[
                RegimeAgent(
                    snapshot_builder=self._build_regime_snapshot,
                    memory=self.agent_memory,
                    event_bus=self.event_bus,
                ),
                PortfolioAgent(
                    snapshot_builder=self._build_portfolio_snapshot,
                    memory=self.agent_memory,
                    event_bus=self.event_bus,
                ),
                TradingRiskAgent(
                    reviewer=self.review_signal,
                    memory=self.agent_memory,
                    event_bus=self.event_bus,
                ),
                TradingExecutionAgent(
                    executor=self.execute_review,
                    memory=self.agent_memory,
                    event_bus=self.event_bus,
                ),
            ]
        )
        self.event_driven_runtime = EventDrivenAgentRuntime(
            bus=self.event_bus,
            signal_agents=self.signal_agents,
            signal_consensus_agent=self.signal_consensus_agent,
            signal_aggregation_agent=self.signal_aggregation_agent,
            regime_agent=self.agent_orchestrator.agents[0],
            portfolio_agent=self.agent_orchestrator.agents[1],
            risk_agent=self.agent_orchestrator.agents[2],
            execution_agent=self.agent_orchestrator.agents[3],
        )

        # =========================
        # SYSTEM SETTINGS
        # =========================

        self.time_frame = getattr(controller, "time_frame", "1h")
        self.limit = getattr(controller, "limit", 50000)
        self.running = False
        self._pipeline_status = {}
        self._rejection_log_cache = {}
        self._adaptive_trade_cache = {
            "expires_at": 0.0,
            "limit": 0,
            "rows": [],
        }

        if self.controller is not None:
            self.controller.agent_memory = self.agent_memory
            self.controller.agent_orchestrator = self.agent_orchestrator
            self.controller.event_bus = self.event_bus
            self.controller.agent_event_runtime = self.event_driven_runtime
            self.controller.signal_agents = self.signal_agents
            self.controller.signal_consensus_agent = self.signal_consensus_agent
            self.controller.signal_aggregation_agent = self.signal_aggregation_agent

        self.bind_agent_decision_repository(getattr(self.controller, "agent_decision_repository", None))
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

    def _resolve_runtime_history_limit(self, limit=None):
        requested = max(1, int(limit or self.limit or 300))
        controller = self.controller
        configured_cap = getattr(controller, "runtime_history_limit", None) if controller is not None else None
        try:
            runtime_cap = max(100, int(configured_cap or self.MAX_RUNTIME_ANALYSIS_BARS))
        except Exception:
            runtime_cap = self.MAX_RUNTIME_ANALYSIS_BARS

        broker_cap = getattr(self.broker, "MAX_OHLCV_COUNT", None)
        try:
            broker_cap = max(1, int(broker_cap)) if broker_cap is not None else None
        except Exception:
            broker_cap = None

        effective_cap = runtime_cap if broker_cap is None else min(runtime_cap, broker_cap)
        return max(1, min(requested, effective_cap))

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

    def _assigned_timeframe_for_symbol(self, symbol, fallback=None):
        assigned = self._assigned_strategies_for_symbol(symbol)
        for row in list(assigned or []):
            timeframe = str(getattr(row, "get", lambda *_args, **_kwargs: None)("timeframe") if hasattr(row, "get") else "" or "").strip()
            if timeframe:
                return timeframe
        return str(fallback or self.time_frame or "1h").strip() or "1h"

    def _select_strategy_signal(self, normalized_symbol, candles, dataset):
        assigned = self._assigned_strategies_for_symbol(normalized_symbol)
        candidates = self._strategy_signal_candidates(normalized_symbol, candles, dataset, assigned)
        if not candidates:
            return None, assigned
        candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return candidates[0][2], assigned

    def _strategy_signal_candidates(self, normalized_symbol, candles, dataset, assignments):
        candidates = []
        for assignment in list(assignments or []):
            strategy_name = str(assignment.get("strategy_name") or "").strip()
            if not strategy_name:
                continue
            assignment_timeframe = str(
                assignment.get("timeframe")
                or getattr(dataset, "timeframe", None)
                or self.time_frame
                or "1h"
            ).strip() or "1h"
            signal = self.signal_engine.generate_signal(
                candles=candles,
                dataset=dataset,
                strategy_name=strategy_name,
                symbol=normalized_symbol,
            )
            if not signal:
                continue
            weighted_confidence = float(signal.get("confidence", 0.0) or 0.0) * max(0.0001, float(assignment.get("weight", 0.0) or 0.0))
            adaptive_profile = self._adaptive_profile_for_strategy(
                normalized_symbol,
                strategy_name,
                timeframe=assignment_timeframe,
            )
            enriched = dict(signal)
            enriched["strategy_name"] = strategy_name
            enriched["timeframe"] = assignment_timeframe
            enriched["strategy_assignment_weight"] = float(assignment.get("weight", 0.0) or 0.0)
            enriched["strategy_assignment_score"] = float(assignment.get("score", 0.0) or 0.0)
            enriched["strategy_assignment_rank"] = int(assignment.get("rank", 0) or 0)
            enriched["adaptive_weight"] = float(adaptive_profile.get("adaptive_weight", 1.0) or 1.0)
            enriched["adaptive_score"] = weighted_confidence * enriched["adaptive_weight"]
            enriched["adaptive_sample_size"] = int(adaptive_profile.get("sample_size", 0) or 0)
            enriched["adaptive_win_rate"] = adaptive_profile.get("win_rate")
            enriched["adaptive_avg_pnl"] = adaptive_profile.get("average_pnl")
            enriched["adaptive_feedback_scope"] = adaptive_profile.get("scope")
            candidates.append((enriched["adaptive_score"], float(assignment.get("score", 0.0) or 0.0), enriched))
        return candidates

    def _normalized_symbol_aliases(self, symbol):
        normalized = str(symbol or "").strip().upper()
        if not normalized:
            return set()
        aliases = {normalized}
        if "/" in normalized:
            aliases.add(normalized.replace("/", "_"))
        if "_" in normalized:
            aliases.add(normalized.replace("_", "/"))
        return aliases

    def _recent_trade_history(self, limit=None):
        requested_limit = max(10, int(limit or self.ADAPTIVE_TRADE_HISTORY_LIMIT))
        repository = getattr(self.controller, "trade_repository", None) if self.controller is not None else None
        if repository is None or not hasattr(repository, "get_trades"):
            return []

        now = time.monotonic()
        cache = dict(self._adaptive_trade_cache or {})
        cached_rows = list(cache.get("rows") or [])
        if (
            cached_rows
            and now < float(cache.get("expires_at", 0.0) or 0.0)
            and int(cache.get("limit", 0) or 0) >= requested_limit
        ):
            return cached_rows[:requested_limit]

        try:
            rows = list(repository.get_trades(limit=requested_limit) or [])
        except Exception:
            self.logger.debug("Unable to load recent trade history for adaptive scoring", exc_info=True)
            rows = []
        self._adaptive_trade_cache = {
            "expires_at": now + self.ADAPTIVE_TRADE_CACHE_TTL_SECONDS,
            "limit": requested_limit,
            "rows": list(rows),
        }
        return rows

    def _trade_feedback_signal(self, trade):
        pnl_value = getattr(trade, "pnl", None)
        pnl = None
        if pnl_value not in (None, ""):
            try:
                pnl = float(pnl_value)
            except Exception:
                pnl = None
        if pnl is not None:
            if pnl > 0:
                return {"score": 1.0, "pnl": pnl}
            if pnl < 0:
                return {"score": -1.0, "pnl": pnl}
            return {"score": 0.0, "pnl": pnl}

        outcome = str(getattr(trade, "outcome", None) or "").strip().lower()
        if outcome:
            if any(token in outcome for token in ("win", "profit", "target", "take profit")):
                return {"score": 1.0, "pnl": None}
            if any(token in outcome for token in ("loss", "losing", "stop", "stopped")):
                return {"score": -1.0, "pnl": None}
            if "break even" in outcome or "breakeven" in outcome:
                return {"score": 0.0, "pnl": None}

        return None

    def _trade_timestamp_value(self, value):
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc).timestamp()
            return value.astimezone(timezone.utc).timestamp()
        if value in (None, ""):
            return None
        try:
            numeric = float(value)
        except Exception:
            numeric = None
        if numeric is not None:
            if abs(numeric) > 1e11:
                numeric = numeric / 1000.0
            return float(numeric)
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except Exception:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).timestamp()

    def _adaptive_profile_for_strategy(self, normalized_symbol, strategy_name, timeframe=None):
        strategy_text = str(strategy_name or "").strip().lower()
        if not strategy_text:
            return {
                "adaptive_weight": 1.0,
                "sample_size": 0,
                "win_rate": None,
                "average_pnl": None,
                "scope": "none",
            }

        symbol_aliases = self._normalized_symbol_aliases(normalized_symbol)
        timeframe_text = str(timeframe or "").strip().lower()
        matched_exact = []
        matched_fallback = []
        for trade in self._recent_trade_history():
            trade_symbol = str(getattr(trade, "symbol", None) or "").strip().upper()
            if symbol_aliases and trade_symbol not in symbol_aliases:
                continue
            if str(getattr(trade, "strategy_name", None) or "").strip().lower() != strategy_text:
                continue
            trade_timeframe = str(getattr(trade, "timeframe", None) or "").strip().lower()
            if timeframe_text and trade_timeframe == timeframe_text:
                matched_exact.append(trade)
            elif not timeframe_text or not trade_timeframe:
                matched_fallback.append(trade)

        scoped_rows = matched_exact or matched_fallback
        feedback = [row for row in (self._trade_feedback_signal(trade) for trade in scoped_rows) if row is not None]
        if not feedback:
            return {
                "adaptive_weight": 1.0,
                "sample_size": 0,
                "win_rate": None,
                "average_pnl": None,
                "scope": "timeframe" if matched_exact else "strategy",
            }

        sample_size = len(feedback)
        average_score = sum(float(item.get("score", 0.0) or 0.0) for item in feedback) / float(sample_size)
        sample_strength = min(1.0, float(sample_size) / 6.0)
        adaptive_weight = 1.0 + (0.35 * average_score * sample_strength)
        adaptive_weight = max(self.ADAPTIVE_WEIGHT_MIN, min(self.ADAPTIVE_WEIGHT_MAX, adaptive_weight))
        pnl_samples = [float(item["pnl"]) for item in feedback if item.get("pnl") is not None]
        wins = sum(1 for item in feedback if float(item.get("score", 0.0) or 0.0) > 0)
        return {
            "adaptive_weight": adaptive_weight,
            "sample_size": sample_size,
            "win_rate": wins / float(sample_size),
            "average_pnl": (sum(pnl_samples) / float(len(pnl_samples))) if pnl_samples else None,
            "scope": "timeframe" if matched_exact else "strategy",
        }

    def adaptive_profile_for_strategy(self, symbol, strategy_name, timeframe=None):
        normalized_symbol = str(symbol or "").strip().upper()
        timeframe_value = str(timeframe or self.time_frame or "1h").strip() or "1h"
        profile = dict(
            self._adaptive_profile_for_strategy(
                normalized_symbol,
                strategy_name,
                timeframe=timeframe_value,
            )
        )
        profile["symbol"] = normalized_symbol
        profile["strategy_name"] = str(strategy_name or "").strip()
        profile["timeframe"] = timeframe_value
        return profile

    def adaptive_trade_samples_for_strategy(self, symbol, strategy_name, timeframe=None, limit=8):
        normalized_symbol = str(symbol or "").strip().upper()
        strategy_text = str(strategy_name or "").strip()
        timeframe_value = str(timeframe or self.time_frame or "1h").strip() or "1h"
        if not normalized_symbol or not strategy_text:
            return {
                "symbol": normalized_symbol,
                "strategy_name": strategy_text,
                "timeframe": timeframe_value,
                "scope": "none",
                "samples": [],
                "profile": {},
            }

        strategy_key = strategy_text.lower()
        symbol_aliases = self._normalized_symbol_aliases(normalized_symbol)
        exact_matches = []
        fallback_matches = []
        scan_limit = max(max(1, int(limit or 8)) * 10, self.ADAPTIVE_TRADE_HISTORY_LIMIT)

        for trade in self._recent_trade_history(limit=scan_limit):
            trade_symbol = str(getattr(trade, "symbol", None) or "").strip().upper()
            if symbol_aliases and trade_symbol not in symbol_aliases:
                continue
            if str(getattr(trade, "strategy_name", None) or "").strip().lower() != strategy_key:
                continue

            feedback = self._trade_feedback_signal(trade)
            if feedback is None:
                continue

            trade_timeframe = str(getattr(trade, "timeframe", None) or "").strip() or ""
            sample = {
                "timestamp": getattr(trade, "timestamp", None),
                "status": str(getattr(trade, "status", None) or "").strip(),
                "side": str(getattr(trade, "side", None) or "").strip(),
                "timeframe": trade_timeframe,
                "pnl": feedback.get("pnl"),
                "score": float(feedback.get("score", 0.0) or 0.0),
                "outcome": str(getattr(trade, "outcome", None) or "").strip(),
                "reason": str(getattr(trade, "reason", None) or "").strip(),
                "source_agent": str(getattr(trade, "signal_source_agent", None) or "").strip(),
                "consensus_status": str(getattr(trade, "consensus_status", None) or "").strip(),
                "adaptive_weight": getattr(trade, "adaptive_weight", None),
                "adaptive_score": getattr(trade, "adaptive_score", None),
            }
            if trade_timeframe.lower() == timeframe_value.lower():
                exact_matches.append(sample)
            elif not trade_timeframe:
                fallback_matches.append(sample)

        samples = list((exact_matches or fallback_matches)[: max(1, int(limit or 8))])
        return {
            "symbol": normalized_symbol,
            "strategy_name": strategy_text,
            "timeframe": timeframe_value,
            "scope": "timeframe" if exact_matches else "strategy",
            "samples": samples,
            "profile": self.adaptive_profile_for_strategy(normalized_symbol, strategy_text, timeframe=timeframe_value),
        }

    def adaptive_weight_timeline_for_strategy(self, symbol, strategy_name, timeframe=None, limit=16):
        detail = self.adaptive_trade_samples_for_strategy(
            symbol,
            strategy_name,
            timeframe=timeframe,
            limit=max(8, int(limit or 16)),
        )
        sample_rows = list(detail.get("samples") or [])
        if not sample_rows:
            return {
                "symbol": detail.get("symbol"),
                "strategy_name": detail.get("strategy_name"),
                "timeframe": detail.get("timeframe"),
                "scope": detail.get("scope"),
                "timeline": [],
                "profile": dict(detail.get("profile") or {}),
            }

        ordered_samples = list(sample_rows)
        ordered_samples.sort(
            key=lambda row: (
                self._trade_timestamp_value(row.get("timestamp")) if isinstance(row, dict) else None
            ) or 0.0
        )

        timeline = []
        running_scores = []
        for index, sample in enumerate(ordered_samples, start=1):
            score = float(sample.get("score", 0.0) or 0.0)
            running_scores.append(score)
            average_score = sum(running_scores) / float(len(running_scores))
            sample_strength = min(1.0, float(len(running_scores)) / 6.0)
            adaptive_weight = 1.0 + (0.35 * average_score * sample_strength)
            adaptive_weight = max(self.ADAPTIVE_WEIGHT_MIN, min(self.ADAPTIVE_WEIGHT_MAX, adaptive_weight))
            timeline.append(
                {
                    "timestamp": sample.get("timestamp"),
                    "timestamp_value": self._trade_timestamp_value(sample.get("timestamp")),
                    "adaptive_weight": adaptive_weight,
                    "score": score,
                    "pnl": sample.get("pnl"),
                    "reason": sample.get("reason"),
                    "side": sample.get("side"),
                    "sample_index": index,
                }
            )

        return {
            "symbol": detail.get("symbol"),
            "strategy_name": detail.get("strategy_name"),
            "timeframe": detail.get("timeframe"),
            "scope": detail.get("scope"),
            "timeline": timeline[-max(1, int(limit or 16)) :],
            "profile": dict(detail.get("profile") or {}),
        }

    def _select_strategy_signal_for_slot(self, slot_index):
        def selector(normalized_symbol, candles, dataset):
            assigned = self._assigned_strategies_for_symbol(normalized_symbol)
            scoped_assignments = list(assigned[slot_index : slot_index + 1])
            candidates = self._strategy_signal_candidates(normalized_symbol, candles, dataset, scoped_assignments)
            if not candidates:
                return None, scoped_assignments
            candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
            return candidates[0][2], scoped_assignments

        return selector

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

    def agent_memory_snapshot(self, limit=50):
        return self.agent_memory.snapshot(limit=limit) if self.agent_memory is not None else []

    def bind_agent_decision_repository(self, repository):
        self.agent_decision_repository = repository
        if self.controller is not None:
            self.controller.agent_decision_repository = repository
        return repository

    def _active_exchange_code(self):
        broker = getattr(self, "broker", None)
        exchange_name = getattr(broker, "exchange_name", None) if broker is not None else None
        if not exchange_name and self.controller is not None:
            exchange_name = getattr(self.controller, "exchange", None)
        return str(exchange_name or "").strip().lower() or None

    def _current_account_label(self):
        resolver = getattr(self.controller, "current_account_label", None) if self.controller is not None else None
        if callable(resolver):
            try:
                label = resolver()
            except Exception:
                label = None
        else:
            label = None
        if str(label or "").strip().lower() == "not set":
            label = None
        return str(label or "").strip() or None

    def _persist_agent_memory_event(self, event):
        repository = getattr(self, "agent_decision_repository", None)
        if repository is None or not hasattr(repository, "save_decision") or not isinstance(event, dict):
            return None
        payload = dict(event.get("payload") or {})
        try:
            return repository.save_decision(
                agent_name=event.get("agent"),
                stage=event.get("stage"),
                symbol=event.get("symbol"),
                decision_id=event.get("decision_id"),
                exchange=self._active_exchange_code(),
                account_label=self._current_account_label(),
                strategy_name=payload.get("strategy_name"),
                timeframe=payload.get("timeframe"),
                side=payload.get("side"),
                confidence=payload.get("confidence"),
                approved=payload.get("approved"),
                reason=payload.get("reason"),
                payload=payload,
                timestamp=event.get("timestamp"),
            )
        except Exception:
            self.logger.debug("Unable to persist agent decision ledger entry", exc_info=True)
            return None

    def _custom_process_signal_handler(self):
        handler = self.__dict__.get("process_signal")
        return handler if callable(handler) else None

    async def _run_signal_agents(self, context):
        working = dict(context or {})
        if len(list(self.signal_agents or [])) <= 1 and self.signal_aggregation_agent is None:
            signal_agent = self.signal_agent
            return await signal_agent.process(working) if signal_agent is not None else working

        working = await run_signal_agents_parallel(self.signal_agents, working)
        if self.signal_consensus_agent is not None:
            working = await self.signal_consensus_agent.process(working)
        if self.signal_aggregation_agent is not None:
            working = await self.signal_aggregation_agent.process(working)
        return working

    def _build_display_signal(self, context, signal, assigned_strategies):
        symbol = str((context or {}).get("symbol") or "").strip().upper()
        default_strategy_name = str(getattr(self.controller, "strategy_name", None) or "Trend Following").strip() or "Trend Following"
        display_strategy_name = signal.get("strategy_name") if isinstance(signal, dict) else None
        if not display_strategy_name:
            display_strategy_name = ", ".join(
                str(item.get("strategy_name") or "").strip()
                for item in list(assigned_strategies or [])[:3]
                if str(item.get("strategy_name") or "").strip()
            ) or default_strategy_name
        if isinstance(signal, dict):
            display_signal = dict(signal)
            display_signal.setdefault("strategy_name", display_strategy_name)
            return display_signal
        if (context or {}).get("blocked_by_news_bias"):
            reason = str((context or {}).get("news_bias_reason") or "Signal was neutralized by news bias controls.").strip()
        else:
            reason = "No entry signal on the latest scan."
        return {
            "symbol": symbol,
            "side": "hold",
            "amount": 0.0,
            "confidence": 0.0,
            "reason": reason,
            "strategy_name": display_strategy_name,
        }

    def _publish_signal_context(self, context, display_signal):
        if not (context or {}).get("publish_debug"):
            return
        features = (context or {}).get("features")
        candles = list((context or {}).get("candles") or [])
        symbol = str((context or {}).get("symbol") or "").strip().upper()
        if self.controller and hasattr(self.controller, "publish_ai_signal"):
            self.controller.publish_ai_signal(symbol, display_signal, candles=candles)
        if self.controller and hasattr(self.controller, "publish_strategy_debug"):
            self.controller.publish_strategy_debug(symbol, display_signal, candles=candles, features=features)

    async def _apply_news_bias(self, symbol, signal):
        if self.controller and hasattr(self.controller, "apply_news_bias_to_signal"):
            return await self.controller.apply_news_bias_to_signal(symbol, signal)
        return signal

    def _feature_frame_for_context(self, candles, dataset=None, strategy_name=None):
        strategy = self.strategy._resolve_strategy(strategy_name) if hasattr(self.strategy, "_resolve_strategy") else self.strategy
        if strategy is None or not hasattr(strategy, "compute_features"):
            return getattr(dataset, "frame", None)
        candle_rows = candles or []
        if not candle_rows and dataset is not None:
            try:
                candle_rows = dataset.to_candles()
            except Exception:
                candle_rows = []
        try:
            return strategy.compute_features(candle_rows)
        except Exception:
            return getattr(dataset, "frame", None)

    def _build_regime_snapshot(self, symbol=None, signal=None, candles=None, dataset=None, timeframe=None):
        feature_frame = self._feature_frame_for_context(candles or [], dataset=dataset, strategy_name=(signal or {}).get("strategy_name") if isinstance(signal, dict) else None)
        regime_engine = getattr(self.signal_engine, "regime_engine", None)
        regime = regime_engine.classify_frame(feature_frame) if regime_engine is not None else "unknown"
        atr_pct = 0.0
        trend_strength = 0.0
        momentum = 0.0
        band_position = 0.5
        if feature_frame is not None and not getattr(feature_frame, "empty", True):
            try:
                row = feature_frame.iloc[-1]
                atr_pct = self._safe_numeric_value(row.get("atr_pct"), 0.0)
                trend_strength = self._safe_numeric_value(row.get("trend_strength"), 0.0)
                momentum = self._safe_numeric_value(row.get("momentum"), 0.0)
                band_position = self._safe_numeric_value(row.get("band_position"), 0.5)
            except Exception:
                pass
        if atr_pct >= 0.03:
            volatility_label = "high"
        elif atr_pct >= 0.015:
            volatility_label = "medium"
        else:
            volatility_label = "low"
        return {
            "symbol": str(symbol or "").strip().upper(),
            "timeframe": str(timeframe or self.time_frame or "1h").strip() or "1h",
            "regime": regime,
            "volatility": volatility_label,
            "atr_pct": atr_pct,
            "trend_strength": trend_strength,
            "momentum": momentum,
            "band_position": band_position,
        }

    def _build_portfolio_snapshot(self, symbol=None):
        portfolio = getattr(self.portfolio, "portfolio", None)
        positions = getattr(portfolio, "positions", {}) or {}
        market_prices = dict(getattr(self.portfolio, "market_prices", {}) or {})
        rows = []
        gross_exposure = 0.0
        net_exposure = 0.0
        for position_symbol, position in positions.items():
            quantity = self._safe_numeric_value(getattr(position, "quantity", 0.0), 0.0)
            if quantity == 0:
                continue
            price = self._safe_numeric_value(market_prices.get(position_symbol), getattr(position, "avg_price", 0.0))
            exposure = quantity * price
            gross_exposure += abs(exposure)
            net_exposure += exposure
            rows.append(
                {
                    "symbol": str(position_symbol).strip().upper(),
                    "quantity": quantity,
                    "avg_price": self._safe_numeric_value(getattr(position, "avg_price", 0.0), 0.0),
                    "last_price": price,
                    "signed_exposure": exposure,
                    "absolute_exposure": abs(exposure),
                }
            )
        try:
            equity = self._safe_numeric_value(self.portfolio.equity(), getattr(self.risk_engine, "account_equity", 0.0) or 0.0)
        except Exception:
            equity = self._safe_numeric_value(getattr(self.risk_engine, "account_equity", 0.0), 0.0)
        try:
            cash = self._safe_numeric_value(getattr(portfolio, "cash", 0.0), 0.0)
        except Exception:
            cash = 0.0
        snapshot = {
            "symbol": str(symbol or "").strip().upper(),
            "equity": equity,
            "cash": cash,
            "gross_exposure": gross_exposure,
            "net_exposure": net_exposure,
            "position_count": len(rows),
            "positions": rows,
        }
        if self.controller is not None:
            self.controller.agent_portfolio_snapshot = dict(snapshot)
        return snapshot

    def _log_rejection_once(self, stage, symbol, reason, template):
        normalized_stage = str(stage or "").strip().lower() or "unknown"
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_reason = str(reason or "").strip() or "Trade rejected."
        cooldown_seconds = float(getattr(self.controller, "rejection_log_cooldown_seconds", 60.0) or 60.0)
        now = datetime.now(timezone.utc)
        cache_key = (normalized_stage, normalized_symbol, normalized_reason)
        previous = self._rejection_log_cache.get(cache_key)
        if previous is not None and (now - previous).total_seconds() < cooldown_seconds:
            return

        stale_before = now - timedelta(seconds=max(cooldown_seconds * 4.0, 300.0))
        self._rejection_log_cache = {
            key: timestamp
            for key, timestamp in self._rejection_log_cache.items()
            if timestamp >= stale_before
        }
        self._rejection_log_cache[cache_key] = now
        self.logger.warning(template, normalized_reason)

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
        target_limit = self._resolve_runtime_history_limit(limit)

        dataset = await self.data_hub.get_symbol_dataset(
            symbol=normalized_symbol,
            timeframe=target_timeframe,
            limit=target_limit,
        )
        candles = dataset.to_candles()
        if not candles:
            self._record_pipeline_status(normalized_symbol, "data_hub", "empty", "No candles returned for symbol")
            return None

        context = {
            "decision_id": uuid4().hex,
            "symbol": normalized_symbol,
            "timeframe": target_timeframe,
            "limit": target_limit,
            "dataset": dataset,
            "candles": candles,
            "features": getattr(dataset, "frame", None),
            "publish_debug": bool(publish_debug),
        }

        custom_handler = self._custom_process_signal_handler()
        if custom_handler is not None:
            context = await self._run_signal_agents(context)
            signal = context.get("signal")
            display_signal = context.get("display_signal") or self._build_display_signal(context, signal, context.get("assigned_strategies") or [])
            if signal:
                self._record_pipeline_status(normalized_symbol, "signal_engine", "signal", signal.get("reason"), signal=signal)
            else:
                if context.get("blocked_by_news_bias"):
                    self._record_pipeline_status(
                        normalized_symbol,
                        "news_bias",
                        "blocked",
                        context.get("news_bias_reason"),
                        signal=display_signal,
                    )
                else:
                    self._record_pipeline_status(
                        normalized_symbol,
                        "signal_engine",
                        "hold",
                        display_signal.get("reason") if isinstance(display_signal, dict) else "No entry signal on the latest scan.",
                        signal=display_signal if isinstance(display_signal, dict) else None,
                    )
                return None
            result = await custom_handler(normalized_symbol, signal, dataset=dataset)
        else:
            context = await self.event_driven_runtime.process_market_data(context)
            signal = context.get("signal")
            display_signal = context.get("display_signal") or self._build_display_signal(context, signal, context.get("assigned_strategies") or [])
            latest_stage = str((self._pipeline_status.get(normalized_symbol) or {}).get("stage") or "").strip()
            if signal:
                if latest_stage in {"", "signal_engine"}:
                    self._record_pipeline_status(normalized_symbol, "signal_engine", "signal", signal.get("reason"), signal=signal)
            else:
                if context.get("blocked_by_news_bias"):
                    self._record_pipeline_status(
                        normalized_symbol,
                        "news_bias",
                        "blocked",
                        context.get("news_bias_reason"),
                        signal=display_signal,
                    )
                else:
                    self._record_pipeline_status(
                        normalized_symbol,
                        "signal_engine",
                        "hold",
                        display_signal.get("reason") if isinstance(display_signal, dict) else "No entry signal on the latest scan.",
                        signal=display_signal if isinstance(display_signal, dict) else None,
                    )
                return None
            result = context.get("execution_result")

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
        await self.event_driven_runtime.start()
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
                        timeframe=self._assigned_timeframe_for_symbol(symbol, fallback=self.time_frame),
                        limit=self.limit,
                        publish_debug=True,
                    )

                await asyncio.sleep(5)

            except Exception:
                self.logger.exception("Trading loop error")

    # ==========================================
    # PROCESS SIGNAL
    # ==========================================

    async def review_signal(self, symbol, signal, dataset=None, timeframe=None, regime_snapshot=None, portfolio_snapshot=None):
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_signal = dict(signal or {})
        review_timeframe = str(timeframe or self.time_frame or "1h").strip() or "1h"
        side = normalized_signal.get("side")
        price = normalized_signal.get("price")
        amount = normalized_signal.get("amount")
        strategy_name = normalized_signal.get("strategy_name") or getattr(self.controller, "strategy_name", "Bot")
        review = {
            "approved": False,
            "symbol": normalized_symbol,
            "signal": normalized_signal,
            "timeframe": review_timeframe,
            "dataset": dataset,
            "strategy_name": strategy_name,
            "signal_source_agent": str(normalized_signal.get("signal_source_agent") or "").strip() or None,
            "consensus_status": str(normalized_signal.get("consensus_status") or "").strip() or None,
            "adaptive_weight": normalized_signal.get("adaptive_weight"),
            "adaptive_score": normalized_signal.get("adaptive_score"),
            "stage": "review",
            "reason": "",
            "portfolio_snapshot": dict(portfolio_snapshot or self._build_portfolio_snapshot(normalized_symbol)),
            "regime_snapshot": dict(regime_snapshot or self._build_regime_snapshot(normalized_symbol, signal=normalized_signal, candles=(dataset.to_candles() if dataset is not None and hasattr(dataset, "to_candles") else []), dataset=dataset, timeframe=review_timeframe)),
        }

        if self.risk_engine is None:
            review["stage"] = "risk_engine"
            review["reason"] = "Risk engine is not initialized."
            self._record_pipeline_status(normalized_symbol, "risk_engine", "rejected", review["reason"], signal=normalized_signal)
            return review

        if (price is None or float(price or 0) <= 0) and dataset is not None and not getattr(dataset, "empty", True):
            try:
                price = float(dataset.frame.iloc[-1]["close"])
            except Exception:
                price = None
        if price is None or float(price or 0) <= 0:
            reason = f"Trade rejected because no executable reference price was available for {normalized_symbol}"
            self.logger.warning(reason)
            review["stage"] = "price_validation"
            review["reason"] = reason
            self._record_pipeline_status(normalized_symbol, "price_validation", "rejected", reason, signal=normalized_signal)
            return review

        canceled_orders, cleanup_reason = await self._cancel_stale_exit_orders(normalized_symbol, side, normalized_signal)
        if canceled_orders:
            self.logger.info("%s", cleanup_reason)
            self._record_pipeline_status(normalized_symbol, "order_cleanup", "approved", cleanup_reason, signal=normalized_signal)
            review["cleanup_reason"] = cleanup_reason
            review["canceled_orders"] = canceled_orders

        basic_reason = "Approved"
        if hasattr(self.risk_engine, "adjust_trade"):
            allowed, adjusted_amount, basic_reason = self.risk_engine.adjust_trade(float(price), float(amount))
        else:
            allowed, basic_reason = self.risk_engine.validate_trade(float(price), float(amount))
            adjusted_amount = float(amount)

        if not allowed:
            self._log_rejection_once("risk_engine", normalized_symbol, basic_reason, "Trade rejected by risk engine: %s")
            self._record_pipeline_status(normalized_symbol, "risk_engine", "rejected", basic_reason, signal=normalized_signal)
            review["stage"] = "risk_engine"
            review["reason"] = basic_reason
            return review
        if adjusted_amount + 1e-12 < float(amount):
            self.logger.info(
                "Risk engine reduced %s order size from %.8f to %.8f: %s",
                normalized_symbol,
                float(amount),
                adjusted_amount,
                basic_reason,
            )
        amount = adjusted_amount
        self._record_pipeline_status(normalized_symbol, "risk_engine", "approved", basic_reason, signal=normalized_signal)

        if self.portfolio_allocator is not None:
            try:
                portfolio_equity = self.portfolio.equity()
            except Exception:
                portfolio_equity = None
            if portfolio_equity:
                self.portfolio_allocator.sync_equity(portfolio_equity)
            allocation = await self.portfolio_allocator.allocate_trade(
                symbol=normalized_symbol,
                strategy_name=strategy_name,
                side=side,
                amount=amount,
                price=price,
                portfolio=getattr(self.portfolio, "portfolio", None),
                market_prices=getattr(self.portfolio, "market_prices", {}),
                dataset=dataset,
                confidence=normalized_signal.get("confidence"),
                active_strategies=[strategy_name],
            )
            if self.controller is not None:
                self.controller.quant_allocation_snapshot = dict(allocation.metrics or {})
            if not allocation.approved:
                self._log_rejection_once(
                    "portfolio_allocator",
                    normalized_symbol,
                    allocation.reason,
                    "Trade rejected by portfolio allocator: %s",
                )
                self._record_pipeline_status(normalized_symbol, "portfolio_allocator", "rejected", allocation.reason, signal=normalized_signal)
                review["stage"] = "portfolio_allocator"
                review["reason"] = allocation.reason
                review["allocation"] = dict(allocation.metrics or {})
                return review
            amount = allocation.adjusted_amount
            self._record_pipeline_status(normalized_symbol, "portfolio_allocator", "approved", allocation.reason, signal=normalized_signal)
            review["allocation"] = dict(allocation.metrics or {})

        if self.portfolio_risk_engine is not None:
            try:
                portfolio_equity = self.portfolio.equity()
            except Exception:
                portfolio_equity = None
            if portfolio_equity:
                self.portfolio_risk_engine.sync_equity(portfolio_equity)
            approval = await self.portfolio_risk_engine.approve_trade(
                symbol=normalized_symbol,
                side=side,
                amount=amount,
                price=price,
                portfolio=getattr(self.portfolio, "portfolio", None),
                market_prices=getattr(self.portfolio, "market_prices", {}),
                data_hub=self.data_hub,
                dataset=dataset,
                timeframe=review_timeframe,
                strategy_name=normalized_signal.get("strategy_name") or getattr(self.controller, "strategy_name", None),
            )
            if self.controller is not None:
                self.controller.quant_risk_snapshot = dict(approval.metrics or {})
            if not approval.approved:
                self._log_rejection_once(
                    "portfolio_risk_engine",
                    normalized_symbol,
                    approval.reason,
                    "Trade rejected by institutional risk engine: %s",
                )
                self._record_pipeline_status(normalized_symbol, "portfolio_risk_engine", "rejected", approval.reason, signal=normalized_signal)
                review["stage"] = "portfolio_risk_engine"
                review["reason"] = approval.reason
                review["portfolio_risk"] = dict(approval.metrics or {})
                return review
            amount = approval.adjusted_amount
            self._record_pipeline_status(normalized_symbol, "portfolio_risk_engine", "approved", approval.reason, signal=normalized_signal)
            review["portfolio_risk"] = dict(approval.metrics or {})

        if self.portfolio_allocator is not None:
            self.portfolio_allocator.register_strategy_symbol(normalized_symbol, strategy_name)

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
            self._log_rejection_once(
                "margin_closeout_guard",
                normalized_symbol,
                reason,
                "Trade rejected by margin closeout guard: %s",
            )
            self._record_pipeline_status(normalized_symbol, "margin_closeout_guard", "rejected", reason, signal=normalized_signal)
            review["stage"] = "margin_closeout_guard"
            review["reason"] = reason
            review["margin_closeout_guard"] = margin_closeout_snapshot
            return review

        execution_strategy = self._resolve_execution_strategy(normalized_symbol, side, amount, price, normalized_signal)
        execution_params = self._execution_params_for_signal(normalized_signal)

        review.update(
            {
                "approved": True,
                "stage": "execution_ready",
                "reason": str(normalized_signal.get("reason") or "Approved for execution.").strip() or "Approved for execution.",
                "side": side,
                "price": float(price),
                "amount": float(amount),
                "type": normalized_signal.get("type", "market"),
                "stop_price": normalized_signal.get("stop_price"),
                "stop_loss": normalized_signal.get("stop_loss"),
                "take_profit": normalized_signal.get("take_profit"),
                "execution_strategy": execution_strategy,
                "execution_params": execution_params,
            }
        )
        return review

    def _review_quantity_mode(self, review, signal):
        for payload in (review, signal):
            if not isinstance(payload, dict):
                continue
            value = str(payload.get("quantity_mode") or "").strip().lower()
            if value:
                return value

        controller = self.controller
        resolver = getattr(controller, "trade_quantity_context", None) if controller is not None else None
        if not callable(resolver):
            return None

        symbol = (
            (review or {}).get("symbol")
            or (signal or {}).get("symbol")
            or ""
        )
        try:
            context = resolver(symbol)
        except Exception:
            self.logger.debug("Unable to resolve quantity mode for %s", symbol, exc_info=True)
            return None

        if isinstance(context, dict) and context.get("supports_lots"):
            value = str(context.get("default_mode") or "lots").strip().lower()
            return value or "lots"
        return None

    async def _preflight_execution_review(self, review, signal):
        controller = self.controller
        preflight = getattr(controller, "_preflight_trade_submission", None) if controller is not None else None
        if not callable(preflight):
            return None

        return await preflight(
            symbol=review.get("symbol"),
            side=review.get("side"),
            amount=review.get("amount"),
            quantity_mode=self._review_quantity_mode(review, signal),
            order_type=review.get("type", "market"),
            price=review.get("price"),
            stop_price=review.get("stop_price"),
            stop_loss=review.get("stop_loss"),
            take_profit=review.get("take_profit"),
        )

    async def _reject_execution_review(self, review, signal, reason):
        normalized_reason = str(reason or "Automated order preflight rejected the trade.").strip()
        submitted_order = {
            "symbol": review.get("symbol"),
            "side": review.get("side"),
            "source": "bot",
            "amount": review.get("amount"),
            "type": review.get("type", "market"),
            "price": review.get("price"),
            "stop_price": review.get("stop_price"),
            "stop_loss": review.get("stop_loss"),
            "take_profit": review.get("take_profit"),
            "strategy_name": review.get("strategy_name"),
            "timeframe": review.get("timeframe"),
            "signal_source_agent": review.get("signal_source_agent"),
            "consensus_status": review.get("consensus_status"),
            "adaptive_weight": review.get("adaptive_weight"),
            "adaptive_score": review.get("adaptive_score"),
            "reason": signal.get("reason") or normalized_reason,
            "confidence": signal.get("confidence"),
            "expected_price": signal.get("price"),
            "pnl": signal.get("pnl"),
            "execution_strategy": review.get("execution_strategy"),
        }
        rejected_execution = {
            "symbol": review.get("symbol"),
            "side": review.get("side"),
            "source": "bot",
            "amount": review.get("amount"),
            "type": review.get("type", "market"),
            "price": review.get("price"),
            "status": "rejected",
            "reason": normalized_reason,
            "raw": {"error": normalized_reason},
        }
        self._record_pipeline_status(
            review.get("symbol"),
            "execution_preflight",
            "rejected",
            normalized_reason,
            signal=signal,
        )
        await self.execution_manager._handle_order_update(
            rejected_execution,
            submitted_order,
            allow_tracking=False,
        )
        return rejected_execution

    async def execute_review(self, review):
        if not isinstance(review, dict) or not review.get("approved"):
            return None

        signal = dict(review.get("signal") or {})
        order_payload = {
            "symbol": review.get("symbol"),
            "side": review.get("side"),
            "amount": review.get("amount"),
            "price": review.get("price"),
            "source": "bot",
            "strategy_name": review.get("strategy_name"),
            "timeframe": review.get("timeframe"),
            "signal_source_agent": review.get("signal_source_agent"),
            "consensus_status": review.get("consensus_status"),
            "adaptive_weight": review.get("adaptive_weight"),
            "adaptive_score": review.get("adaptive_score"),
            "reason": signal.get("reason"),
            "confidence": signal.get("confidence"),
            "expected_price": signal.get("price"),
            "pnl": signal.get("pnl"),
            "execution_strategy": review.get("execution_strategy"),
            "type": review.get("type", "market"),
            "stop_price": review.get("stop_price"),
            "stop_loss": review.get("stop_loss"),
            "take_profit": review.get("take_profit"),
            "params": review.get("execution_params"),
        }

        try:
            preflight = await self._preflight_execution_review(review, signal)
        except RuntimeError as exc:
            return await self._reject_execution_review(review, signal, str(exc))
        except Exception as exc:
            self.logger.exception(
                "Automated order preflight failed for %s",
                review.get("symbol"),
            )
            return await self._reject_execution_review(
                review,
                signal,
                f"Automated order preflight failed before broker submission: {exc}",
            )

        if isinstance(preflight, dict):
            order_payload["amount"] = float(preflight.get("amount_units", order_payload["amount"]))
            for key in (
                "requested_amount",
                "requested_mode",
                "requested_amount_units",
                "deterministic_amount_units",
                "amount_units",
                "applied_requested_mode_amount",
                "size_adjusted",
                "ai_adjusted",
                "sizing_summary",
                "sizing_notes",
                "ai_sizing_reason",
            ):
                value = preflight.get(key)
                if value not in (None, "", []):
                    payload_key = "requested_quantity_mode" if key == "requested_mode" else key
                    order_payload[payload_key] = value

        order = await self.execution_manager.execute(**order_payload)
        return order

    async def process_signal(self, symbol, signal, dataset=None, timeframe=None, regime_snapshot=None, portfolio_snapshot=None):
        review = await self.review_signal(
            symbol=symbol,
            signal=signal,
            dataset=dataset,
            timeframe=timeframe,
            regime_snapshot=regime_snapshot,
            portfolio_snapshot=portfolio_snapshot,
        )
        if not review.get("approved"):
            return None
        return await self.execute_review(review)

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

        event_runtime = getattr(self, "event_driven_runtime", None)
        if event_runtime is not None:
            try:
                await event_runtime.stop()
            except Exception:
                self.logger.exception("Event-driven runtime stop failed")

        execution_manager = getattr(self, "execution_manager", None)
        if execution_manager is not None:
            try:
                await execution_manager.stop()
            except Exception:
                self.logger.exception("Execution manager stop failed")
