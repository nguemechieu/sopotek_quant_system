from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Iterable, Protocol

from sopotek.core.event_bus.event import Event


class EventStore(Protocol):
    async def append(self, event: Event) -> None:
        ...

    async def read(
        self,
        *,
        event_types: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> list[Event]:
        ...


class InMemoryEventStore:
    def __init__(self) -> None:
        self._events: list[dict[str, object]] = []
        self._lock = asyncio.Lock()

    async def append(self, event: Event) -> None:
        async with self._lock:
            self._events.append(event.to_record())

    async def read(
        self,
        *,
        event_types: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> list[Event]:
        async with self._lock:
            records = list(self._events)
        if event_types is not None:
            allowed = {str(item) for item in event_types}
            records = [row for row in records if row.get("type") in allowed]
        if limit is not None and limit >= 0:
            records = records[-limit:]
        return [Event.from_record(record) for record in records]


class JsonlEventStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    async def append(self, event: Event) -> None:
        payload = json.dumps(event.to_record(), default=str)
        async with self._lock:
            await asyncio.to_thread(self._append_line, payload)

    def _append_line(self, payload: str) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(payload + "\n")

    async def read(
        self,
        *,
        event_types: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> list[Event]:
        async with self._lock:
            lines = await asyncio.to_thread(self._read_lines)
        records = [json.loads(line) for line in lines if line.strip()]
        if event_types is not None:
            allowed = {str(item) for item in event_types}
            records = [row for row in records if row.get("type") in allowed]
        if limit is not None and limit >= 0:
            records = records[-limit:]
        return [Event.from_record(record) for record in records]

    def _read_lines(self) -> list[str]:
        if not self.path.exists():
            return []
        with self.path.open("r", encoding="utf-8") as handle:
            return handle.readlines()
