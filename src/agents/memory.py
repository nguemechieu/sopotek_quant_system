from collections import defaultdict, deque
from datetime import datetime, timezone


class AgentMemory:
    def __init__(self, max_events=2000, sinks=None):
        self.max_events = max(10, int(max_events or 2000))
        self._events = deque(maxlen=self.max_events)
        self._by_agent = defaultdict(lambda: deque(maxlen=self.max_events))
        self._sinks = []
        for sink in list(sinks or []):
            self.add_sink(sink)

    def add_sink(self, sink):
        if callable(sink) and sink not in self._sinks:
            self._sinks.append(sink)
        return sink

    def store(self, agent, stage, payload=None, symbol=None, decision_id=None):
        event = {
            "agent": str(agent or "unknown").strip() or "unknown",
            "stage": str(stage or "unknown").strip() or "unknown",
            "symbol": str(symbol or "").strip().upper(),
            "decision_id": str(decision_id or "").strip() or None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": dict(payload or {}),
        }
        self._events.append(event)
        self._by_agent[event["agent"]].append(event)
        for sink in list(self._sinks):
            try:
                sink(dict(event))
            except Exception:
                continue
        return dict(event)

    def latest(self, agent=None):
        if agent:
            events = self._by_agent.get(str(agent), ())
            return dict(events[-1]) if events else None
        return dict(self._events[-1]) if self._events else None

    def snapshot(self, limit=None, agent=None):
        if agent:
            events = list(self._by_agent.get(str(agent), ()))
        else:
            events = list(self._events)
        if limit is not None:
            events = events[-max(0, int(limit)):]
        return [dict(event) for event in events]
