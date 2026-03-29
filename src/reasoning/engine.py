from __future__ import annotations

import asyncio

from reasoning.context_builder import ReasoningContextBuilder
from reasoning.prompt_engine import PromptEngine
from reasoning.providers import HeuristicReasoningProvider
from reasoning.schema import ReasoningResult


class ReasoningEngine:
    def __init__(
        self,
        *,
        provider=None,
        fallback_provider=None,
        context_builder=None,
        prompt_engine=None,
        enabled: bool = True,
        mode: str = "assistive",
        minimum_confidence: float = 0.75,
        timeout_seconds: float = 8.0,
    ) -> None:
        self.provider = provider
        self.fallback_provider = fallback_provider or HeuristicReasoningProvider()
        self.context_builder = context_builder or ReasoningContextBuilder()
        self.prompt_engine = prompt_engine or PromptEngine()
        self.enabled = bool(enabled)
        self.mode = str(mode or "assistive").strip().lower() or "assistive"
        if self.mode not in {"assistive", "advisory", "autonomous"}:
            self.mode = "assistive"
        self.minimum_confidence = max(0.0, min(1.0, float(minimum_confidence or 0.75)))
        self.timeout_seconds = max(1.0, float(timeout_seconds or 8.0))

    async def evaluate(
        self,
        *,
        symbol: str,
        signal: dict,
        dataset=None,
        timeframe: str = "1h",
        regime_snapshot=None,
        portfolio_snapshot=None,
        risk_limits=None,
    ) -> tuple[ReasoningResult | None, dict]:
        context = self.context_builder.build(
            symbol=symbol,
            signal=signal,
            dataset=dataset,
            timeframe=timeframe,
            regime_snapshot=regime_snapshot,
            portfolio_snapshot=portfolio_snapshot,
            risk_limits=risk_limits,
        )
        if not self.enabled:
            return None, context

        messages = self.prompt_engine.build_messages(context, mode=self.mode)
        provider = self.provider or self.fallback_provider
        used_fallback = False

        try:
            result = await asyncio.wait_for(
                provider.evaluate(messages=messages, context=context, mode=self.mode),
                timeout=self.timeout_seconds,
            )
        except Exception:
            if provider is self.fallback_provider:
                raise
            used_fallback = True
            result = await asyncio.wait_for(
                self.fallback_provider.evaluate(messages=messages, context=context, mode=self.mode),
                timeout=self.timeout_seconds,
            )

        if not isinstance(result, ReasoningResult):
            result = ReasoningResult.from_payload(result or {}, mode=self.mode)

        result.fallback_used = bool(result.fallback_used or used_fallback)
        result.should_execute = self.should_execute(result)
        return result, context

    def should_execute(self, result: ReasoningResult) -> bool:
        if self.mode == "assistive":
            return True
        if str(result.decision or "").strip().upper() == "REJECT":
            return False
        if float(result.confidence or 0.0) < self.minimum_confidence:
            return False
        return True
