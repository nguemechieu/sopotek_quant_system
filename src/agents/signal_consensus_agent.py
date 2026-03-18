from agents.base_agent import BaseAgent


class SignalConsensusAgent(BaseAgent):
    def __init__(self, minimum_votes=2, memory=None, event_bus=None):
        super().__init__("SignalConsensusAgent", memory=memory, event_bus=event_bus)
        self.minimum_votes = max(1, int(minimum_votes or 2))

    def _weighted_score(self, candidate):
        signal = dict((candidate or {}).get("signal") or {})
        return float(signal.get("confidence", 0.0) or 0.0) * max(
            0.0001, float(signal.get("strategy_assignment_weight", 0.0) or 0.0)
        )

    async def process(self, context):
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
            agent_name = str(candidate.get("agent_name") or "").strip()
            if agent_name:
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

        status = "split"
        if len(ranked_votes) == 1:
            status = "unanimous"
        elif int(winner["count"]) > int(runner_up["count"]):
            status = "majority"
        elif float(winner["weighted_score"]) > float(runner_up["weighted_score"]):
            status = "weighted"

        consensus = {
            "status": status,
            "side": winner_side if status != "split" else "",
            "vote_count": int(winner["count"]),
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

        filtered_candidates = list(candidates)
        if status != "split" and int(winner["count"]) >= self.minimum_votes:
            filtered_candidates = [
                candidate
                for candidate in candidates
                if str(dict(candidate.get("signal") or {}).get("side") or "").strip().lower() == winner_side
            ]
            working["signal_candidates"] = filtered_candidates

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
