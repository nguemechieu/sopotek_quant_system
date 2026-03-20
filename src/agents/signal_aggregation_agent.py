from agents.base_agent import BaseAgent


class SignalAggregationAgent(BaseAgent):
    def __init__(self, display_builder=None, publisher=None, memory=None, event_bus=None):
        super().__init__("SignalAggregationAgent", memory=memory, event_bus=event_bus)
        self.display_builder = display_builder
        self.publisher = publisher

    def _candidate_rank(self, candidate):
        signal = dict((candidate or {}).get("signal") or {})
        base_weighted_confidence = float(signal.get("confidence", 0.0) or 0.0) * max(
            0.0001, float(signal.get("strategy_assignment_weight", 0.0) or 0.0)
        )
        adaptive_score = float(signal.get("adaptive_score", base_weighted_confidence) or base_weighted_confidence)
        return (
            adaptive_score,
            float(signal.get("adaptive_weight", 1.0) or 1.0),
            float(signal.get("strategy_assignment_score", 0.0) or 0.0),
            float(signal.get("confidence", 0.0) or 0.0),
            -int(signal.get("strategy_assignment_rank", 0) or 0),
        )

    async def process(self, context):
        working = dict(context or {})
        symbol = str(working.get("symbol") or "").strip().upper()
        decision_id = working.get("decision_id")
        assigned_strategies = list(working.get("assigned_strategies") or [])
        candidates = [
            dict(candidate)
            for candidate in list(working.get("signal_candidates") or [])
            if isinstance(candidate, dict) and isinstance(candidate.get("signal"), dict)
        ]

        if not candidates:
            working["signal"] = None
            display_signal = (
                self.display_builder(working, None, assigned_strategies)
                if callable(self.display_builder)
                else None
            )
            working["display_signal"] = display_signal
            if callable(self.publisher):
                self.publisher(working, display_signal)
            self.remember(
                "hold",
                {
                    "reason": str(working.get("news_bias_reason") or "No entry signal on the latest scan.").strip(),
                    "timeframe": working.get("timeframe"),
                    "candidate_count": 0,
                },
                symbol=symbol,
                decision_id=decision_id,
            )
            return working

        ranked_candidates = sorted(candidates, key=self._candidate_rank, reverse=True)
        selected_candidate = ranked_candidates[0]
        signal = dict(selected_candidate.get("signal") or {})
        signal["signal_source_agent"] = str(selected_candidate.get("agent_name") or "").strip()
        signal["consensus_status"] = str((working.get("signal_consensus") or {}).get("status") or "").strip()
        signal["consensus_side"] = str((working.get("signal_consensus") or {}).get("side") or "").strip()
        working["signal"] = signal
        working["signal_source_agent"] = signal.get("signal_source_agent")
        working["assigned_strategies"] = assigned_strategies or list(selected_candidate.get("assigned_strategies") or [])
        display_signal = (
            self.display_builder(working, signal, working["assigned_strategies"])
            if callable(self.display_builder)
            else signal
        )
        working["display_signal"] = display_signal
        if callable(self.publisher):
            self.publisher(working, display_signal)
        self.remember(
            "selected",
            {
                "strategy_name": signal.get("strategy_name"),
                "timeframe": working.get("timeframe"),
                "side": signal.get("side"),
                "confidence": signal.get("confidence"),
                "reason": signal.get("reason"),
                "source_agent": working.get("signal_source_agent"),
                "candidate_count": len(candidates),
                "consensus_side": str((working.get("signal_consensus") or {}).get("side") or "").strip(),
                "consensus_status": str((working.get("signal_consensus") or {}).get("status") or "").strip(),
                "adaptive_weight": signal.get("adaptive_weight"),
                "adaptive_score": signal.get("adaptive_score"),
                "adaptive_sample_size": signal.get("adaptive_sample_size"),
            },
            symbol=symbol,
            decision_id=decision_id,
        )
        return working
