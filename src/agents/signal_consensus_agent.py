from agents.base_agent import BaseAgent


class SignalConsensusAgent(BaseAgent):
    """Aggregate signal candidates and produce a consensus voting result.

    This agent collects candidate signals from multiple signal generators and
    evaluates whether a shared trading side has enough votes to be accepted.
    The output includes a consensus summary and, when a winner is confirmed,
    it filters the passed candidates to keep only those matching the agreed side.
    """

    def __init__(self, minimum_votes=2, memory=None, event_bus=None):
        super().__init__("SignalConsensusAgent", memory=memory, event_bus=event_bus)
        self.minimum_votes = max(1, int(minimum_votes or 2))

    def _weighted_score(self, candidate):
        """Compute a weighted score for a signal candidate.

        The score is the product of confidence and strategy-assignment weight. A
        small epsilon is used to avoid zero weighting when the assignment weight is
        missing or zero.

        Args:
            candidate: A mapping-like signal candidate that may contain a nested
                "signal" dict with confidence and strategy_assignment_weight.

        Returns:
            A float score used to break ties and compare candidate strength.
        """
        signal = dict((candidate or {}).get("signal") or {})
        return float(signal.get("confidence", 0.0) or 0.0) * max(
            0.0001, float(signal.get("strategy_assignment_weight", 0.0) or 0.0)
        )

    async def process(self, context):
        """Build consensus from signal candidates and optionally filter winners.

        Args:
            context: A dictionary that should contain symbol, decision_id, and
                signal_candidates. Each candidate is expected to be a dict with a
                nested "signal" dict.

        Returns:
            The updated context dict with a "signal_consensus" entry and,
            when a consensus winner exists, a filtered "signal_candidates" list.
        """
        working = dict(context or {})
        symbol = str(working.get("symbol") or "").strip().upper()
        decision_id = working.get("decision_id")
        candidates = [
            dict(candidate)
            for candidate in list(working.get("signal_candidates") or [])
            if isinstance(candidate, dict) and isinstance(candidate.get("signal"), dict)
        ]
        if not candidates:
            return working

        vote_table = {}
        for candidate in candidates:
            signal = dict(candidate.get("signal") or {})
            side = str(signal.get("side") or "").strip().lower()
            if not side:
                continue

            bucket = vote_table.setdefault(
                side,
                {
                    "count": 0,
                    "weighted_score": 0.0,
                    "agents": [],
                },
            )
            bucket["count"] += 1
            bucket["weighted_score"] += self._weighted_score(candidate)
            if agent_name := str(candidate.get("agent_name") or "").strip():
                bucket["agents"].append(agent_name)

        if not vote_table:
            return working

        ranked_votes = sorted(
            vote_table.items(),
            key=lambda item: (int(item[1]["count"]), float(item[1]["weighted_score"])),
            reverse=True,
        )
        winner_side, winner = ranked_votes[0]
        runner_up = ranked_votes[1][1] if len(ranked_votes) > 1 else None

        winner_count = int(winner["count"])
        winner_weight = float(winner["weighted_score"])

        status = "split"
        if winner_count < self.minimum_votes:
            status = "split"
        elif len(ranked_votes) == 1:
            status = "unanimous"
        elif runner_up is None:
            status = "majority"
        elif winner_count > int(runner_up["count"]):
            status = "majority"
        elif winner_weight > float(runner_up["weighted_score"]):
            status = "weighted"

        consensus = {
            "status": status,
            "side": winner_side if status != "split" else "",
            "vote_count": winner_count,
            "total_candidates": len(candidates),
            "minimum_votes": self.minimum_votes,
            "votes": {
                side: {
                    "count": int(values["count"]),
                    "weighted_score": float(values["weighted_score"]),
                    "agents": list(values["agents"]),
                }
                for side, values in vote_table.items()
            },
        }
        working["signal_consensus"] = consensus

        if status != "split" and winner_count >= self.minimum_votes:
            working["signal_candidates"] = [
                candidate
                for candidate in candidates
                if str(dict(candidate.get("signal") or {}).get("side") or "").strip().lower() == winner_side
            ]

        self.remember(
            status,
            {
                "side": consensus["side"] or "mixed",
                "vote_count": consensus["vote_count"],
                "total_candidates": consensus["total_candidates"],
                "minimum_votes": self.minimum_votes,
            },
            symbol=symbol,
            decision_id=decision_id,
        )
        return working
