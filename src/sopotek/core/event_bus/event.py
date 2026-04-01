from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


class Event:
    """Compatibility-friendly event envelope for the v2 runtime."""

    def __init__(
        self,
        event_type: str | None = None,
        data: Any = None,
        timestamp: datetime | None = None,
        *,
        priority: int = 100,
        event_id: str | None = None,
        source: str | None = None,
        correlation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        replayed: bool = False,
        sequence: int | None = None,
        **kwargs: Any,
    ) -> None:
        if event_type is None:
            event_type = kwargs.get("type")
        if data is None and "data" in kwargs:
            data = kwargs.get("data")

        normalized_timestamp = timestamp or kwargs.get("timestamp")
        if normalized_timestamp is None:
            normalized_timestamp = datetime.now(timezone.utc)
        elif normalized_timestamp.tzinfo is None:
            normalized_timestamp = normalized_timestamp.replace(tzinfo=timezone.utc)

        self.type = event_type
        self.data = data
        self.timestamp = normalized_timestamp
        self.priority = int(kwargs.get("priority", priority))
        self.id = str(kwargs.get("id") or event_id or uuid4().hex)
        self.source = source or kwargs.get("source")
        self.correlation_id = correlation_id or kwargs.get("correlation_id")
        self.metadata = dict(metadata or kwargs.get("metadata") or {})
        self.replayed = bool(kwargs.get("replayed", replayed))
        self.sequence = kwargs.get("sequence", sequence)

    def copy(self, **updates: Any) -> "Event":
        payload = {
            "event_type": self.type,
            "data": self.data,
            "timestamp": self.timestamp,
            "priority": self.priority,
            "event_id": self.id,
            "source": self.source,
            "correlation_id": self.correlation_id,
            "metadata": dict(self.metadata),
            "replayed": self.replayed,
            "sequence": self.sequence,
        }
        payload.update(updates)
        return Event(**payload)

    def to_record(self) -> dict[str, Any]:
        data = self.data
        if is_dataclass(data):
            data = asdict(data)
        return {
            "id": self.id,
            "type": self.type,
            "data": data,
            "timestamp": self.timestamp.isoformat(),
            "priority": self.priority,
            "source": self.source,
            "correlation_id": self.correlation_id,
            "metadata": dict(self.metadata),
            "replayed": self.replayed,
            "sequence": self.sequence,
        }

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "Event":
        timestamp = record.get("timestamp")
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        return cls(
            event_type=record.get("type"),
            data=record.get("data"),
            timestamp=timestamp,
            priority=record.get("priority", 100),
            event_id=record.get("id"),
            source=record.get("source"),
            correlation_id=record.get("correlation_id"),
            metadata=record.get("metadata"),
            replayed=record.get("replayed", False),
            sequence=record.get("sequence"),
        )
