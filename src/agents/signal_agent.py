"""Signal selection agent for symbol-level role assignment and candidate filtering.

This module defines a signal agent that delegates decision logic to a selector
callable and optionally enriches the resulting output for display, publishing,
news bias control, and memory recording.
"""

import inspect

from agents.base_agent import BaseAgent


class SignalAgent(BaseAgent):
    """Agent that selects and normalizes trading signals for a symbol.

    The signal agent uses a selector callable to produce a raw signal and an
    optional set of assigned strategies. It supports candidate mode for strategy
    aggregation, applies news bias gating, and records decisions in agent
    memory for downstream execution or analysis.
    """

    def __init__(
        self,
        selector,
        name=None,
        display_builder=None,
        publisher=None,
        news_bias_applier=None,
        memory=None,
        event_bus=None,
        candidate_mode=False,
    ):
        """Initialize a signal agent.

        Parameters:
            selector: Callable that returns (signal, assigned_strategies) for a
                symbol, candle history, and dataset.
            name: Optional human-readable agent name.
            display_builder: Optional callable for creating a display-ready
                representation from working context, signal, and assignments.
            publisher: Optional callable for publishing the final working context
                and display signal to an external sink.
            news_bias_applier: Optional callable for applying news-based bias
                checks to the raw signal before it is accepted.
            memory: Optional memory backend used by the base agent.
            event_bus: Optional event bus used by the base agent.
            candidate_mode: When True, the agent accumulates candidate strategy
                assignments instead of treating the signal as a final selection.
        """
        super().__init__(name or "SignalAgent", memory=memory, event_bus=event_bus)
        self.selector = selector
        self.display_builder = display_builder
        self.publisher = publisher
        self.news_bias_applier = news_bias_applier
        self.candidate_mode = bool(candidate_mode)

    async def process(self, context):
        """Process a working context and enrich it with signal data.

        The context should provide `symbol` and may include additional runtime
        values such as `candles`, `dataset`, `timeframe`, and `decision_id`.
        The returned context is a copy of the input, updated with the selected
        signal, assigned strategies, display signal, and memory recording info.
        """
        working = dict(context or {})
        # Normalize the inbound symbol and preserve the decision identifier.
        symbol = str(working.get("symbol") or "").strip().upper()
        decision_id = working.get("decision_id")
        candles = working.get("candles") or []
        dataset = working.get("dataset")

        selection = self.selector(symbol, candles, dataset)
        if inspect.isawaitable(selection):
            selection = await selection
        signal, assigned_strategies = selection

        # Candidate mode merges historical assignments; normal mode replaces them.
        if self.candidate_mode:
            merged_assignments = list(working.get("assigned_strategies") or [])
            known_assignments = {
                (
                    str(row.get("strategy_name") or "").strip(),
                    str(row.get("timeframe") or "").strip(),
                )
                for row in merged_assignments
                if isinstance(row, dict)
            }
            for assignment in list(assigned_strategies or []):
                if not isinstance(assignment, dict):
                    continue
                fingerprint = (
                    str(assignment.get("strategy_name") or "").strip(),
                    str(assignment.get("timeframe") or "").strip(),
                )
                if fingerprint in known_assignments:
                    continue
                merged_assignments.append(dict(assignment))
                known_assignments.add(fingerprint)
            working["assigned_strategies"] = merged_assignments
        else:
            working["assigned_strategies"] = list(assigned_strategies or [])

        # Apply news bias gating if configured, which may neutralize the signal.
        if signal is not None and callable(self.news_bias_applier):
            bias_result = self.news_bias_applier(symbol, signal)
            if inspect.isawaitable(bias_result):
                bias_result = await bias_result
            signal = bias_result
            if not signal:
                working["blocked_by_news_bias"] = True
                working["news_bias_reason"] = "Signal was neutralized by news bias controls."

        working["signal"] = signal
        display_signal = self.display_builder(working, signal, working["assigned_strategies"]) if callable(self.display_builder) else signal
        working["display_signal"] = display_signal

        # Publish the final working context and display signal if an external
        # publisher is configured.
        if callable(self.publisher):
            self.publisher(working, display_signal)

        # If no valid signal remains, record a blocked or hold decision and
        # return the current working context.
        if signal is None:
            if self.candidate_mode and not working["assigned_strategies"]:
                return working
            stage = "blocked" if working.get("blocked_by_news_bias") else "hold"
            reason = ""
            if isinstance(display_signal, dict):
                reason = str(display_signal.get("reason") or "").strip()
            if not reason:
                reason = str(working.get("news_bias_reason") or "No entry signal on the latest scan.").strip()
            self.remember(
                stage,
                {
                    "reason": reason,
                    "assigned_count": len(working["assigned_strategies"]),
                    "timeframe": working.get("timeframe"),
                },
                symbol=symbol,
                decision_id=decision_id,
            )
            return working

        # Candidate mode stores the signal as a candidate entry instead of
        # committing it as the final selected signal.
        if self.candidate_mode:
            signal_candidate = signal.copy() if isinstance(signal, dict) else {"value": signal}
            if isinstance(signal_candidate, dict):
                signal_candidate.setdefault("timeframe", working.get("timeframe"))
            working.setdefault("signal_candidates", []).append(
                {
                    "agent_name": self.name,
                    "signal": signal_candidate,
                    "assigned_strategies": list(assigned_strategies or []),
                    "timeframe": working.get("timeframe"),
                    "strategy_name": signal_candidate.get("strategy_name"),
                    "confidence": signal_candidate.get("confidence"),
                    "side": signal_candidate.get("side"),
                    "reason": signal_candidate.get("reason"),
                }
            )
            self.remember(
                "candidate",
                {
                    "strategy_name": signal_candidate.get("strategy_name"),
                    "timeframe": working.get("timeframe"),
                    "side": signal_candidate.get("side"),
                    "confidence": signal_candidate.get("confidence"),
                    "reason": signal_candidate.get("reason"),
                    "assigned_count": len(working["assigned_strategies"]),
                    "adaptive_weight": signal_candidate.get("adaptive_weight"),
                    "adaptive_score": signal_candidate.get("adaptive_score"),
                    "adaptive_sample_size": signal_candidate.get("adaptive_sample_size"),
                },
                symbol=symbol,
                decision_id=decision_id,
            )
            return working

        # Record the selected signal details to agent memory for audit and
        # downstream decision tracking.
        signal_mapping = signal if isinstance(signal, dict) else {}
        self.remember(
            "selected",
            {
                "strategy_name": signal_mapping.get("strategy_name"),
                "timeframe": working.get("timeframe"),
                "side": signal_mapping.get("side"),
                "confidence": signal_mapping.get("confidence"),
                "reason": signal_mapping.get("reason"),
                "assigned_count": len(working["assigned_strategies"]),
            },
            symbol=symbol,
            decision_id=decision_id,
        )
        return working
