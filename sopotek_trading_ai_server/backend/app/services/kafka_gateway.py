from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any


KafkaHandler = Callable[[str, dict[str, Any]], Awaitable[None]]


class BaseKafkaGateway(ABC):
    @abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def publish(self, topic: str, payload: dict[str, Any], *, key: str | None = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def subscribe(self, topic: str, handler: KafkaHandler) -> None:
        raise NotImplementedError


class InMemoryKafkaGateway(BaseKafkaGateway):
    def __init__(self) -> None:
        self._handlers: dict[str, list[KafkaHandler]] = defaultdict(list)
        self.published_messages: list[dict[str, Any]] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def subscribe(self, topic: str, handler: KafkaHandler) -> None:
        self._handlers[str(topic)].append(handler)

    async def publish(self, topic: str, payload: dict[str, Any], *, key: str | None = None) -> None:
        message = {"topic": str(topic), "key": key, "payload": payload}
        self.published_messages.append(message)
        for handler in list(self._handlers.get(str(topic), [])):
            await handler(str(topic), dict(payload))


class AioKafkaGateway(BaseKafkaGateway):
    def __init__(self, settings) -> None:
        self.settings = settings
        self._handlers: dict[str, list[KafkaHandler]] = defaultdict(list)
        self._consumer_task: asyncio.Task | None = None
        self._producer = None
        self._consumer = None

    async def start(self) -> None:
        try:
            from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
        except ImportError as exc:
            raise RuntimeError("aiokafka is required for Kafka-backed deployment") from exc

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            client_id=self.settings.kafka_client_id,
            value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8") if value else None,
        )
        self._consumer = AIOKafkaConsumer(
            self.settings.kafka_market_topic,
            self.settings.kafka_execution_topic,
            self.settings.kafka_portfolio_topic,
            self.settings.kafka_risk_topic,
            self.settings.kafka_strategy_state_topic,
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            client_id=self.settings.kafka_client_id,
            group_id=self.settings.kafka_group_id,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        await self._producer.start()
        await self._consumer.start()
        self._consumer_task = asyncio.create_task(self._consume_loop())

    async def stop(self) -> None:
        if self._consumer_task is not None:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        if self._consumer is not None:
            await self._consumer.stop()
        if self._producer is not None:
            await self._producer.stop()

    def subscribe(self, topic: str, handler: KafkaHandler) -> None:
        self._handlers[str(topic)].append(handler)

    async def publish(self, topic: str, payload: dict[str, Any], *, key: str | None = None) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer has not been started")
        await self._producer.send_and_wait(str(topic), dict(payload), key=key)

    async def _consume_loop(self) -> None:
        if self._consumer is None:
            return
        async for message in self._consumer:
            handlers = list(self._handlers.get(str(message.topic), []))
            for handler in handlers:
                await handler(str(message.topic), dict(message.value or {}))


def build_kafka_gateway(settings):
    if settings.is_memory_kafka:
        return InMemoryKafkaGateway()
    return AioKafkaGateway(settings)
