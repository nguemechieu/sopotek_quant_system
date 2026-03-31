"""In-memory event memory for agent execution and decision tracking.

This module provides a lightweight event store for agents, with optional
sink callbacks that are notified whenever a new event is recorded. The memory
supports per-agent lookup and snapshot export for debugging, telemetry, and
replay workflows.
"""

from collections import defaultdict, deque
from datetime import datetime, timezone


class AgentMemory:
    """Async-compatible in-memory event store for agent activity."""

    def __init__(self, max_events=2000, sinks=None):
        """Initialize memory with a bounded event history and optional sinks.

        Parameters:
            max_events: Maximum number of total events to retain.
            sinks: Optional iterable of callable sinks to receive event copies.
        """
        self.max_events = max(10, int(max_events or 2000))
        self._events = deque(maxlen=self.max_events)
        self._by_agent = defaultdict(lambda: deque(maxlen=self.max_events))
        self._sinks = []
        for sink in list(sinks or []):
            self.add_sink(sink)

    def add_sink(self, sink):
        """Register a new sink callback that receives copies of stored events."""
        if callable(sink) and sink not in self._sinks:
            self._sinks.append(sink)
        return sink

    def store(self, agent, stage, payload=None, symbol=None, decision_id=None):
        """Store a new agent event and notify any registered sinks.

        The event is normalized before storage so callers can rely on consistent
        fields and value types.
        """
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
        """Return the latest event globally or for a specific agent."""
        if agent:
            events = self._by_agent.get(str(agent), ())
            return dict(events[-1]) if events else None
        return dict(self._events[-1]) if self._events else None

    def snapshot(self, limit=None, agent=None):
        """Return a copy of recent events, optionally limited or agent-scoped."""
        if agent:
            events = list(self._by_agent.get(str(agent), ()))
        else:
            events = list(self._events)
        if limit is not None:
            events = events[-max(0, int(limit)):]
        return [dict(event) for event in events]
