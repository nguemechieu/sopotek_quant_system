from __future__ import annotations

import asyncio

from reasoning.context_builder import ReasoningContextBuilder
from reasoning.prompt_engine import PromptEngine
from reasoning.providers import HeuristicReasoningProvider
from reasoning.schema import ReasoningResult


class ReasoningEngine:
    """Evaluate trading signals through a reasoning provider and returned decision context."""

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
        """Initialize the reasoning engine.

        Args:
            provider: Primary reasoning provider instance.
            fallback_provider: Provider used when the primary provider fails.
            context_builder: Builder that assembles the reasoning context.
            prompt_engine: Engine used to render reasoning prompts.
            enabled: Whether reasoning evaluation is enabled.
            mode: Evaluation mode: assistive, advisory, or autonomous.
            minimum_confidence: Minimum confidence threshold for execution.
            timeout_seconds: Provider call timeout in seconds.
        """
        self.provider = provider
        self.fallback_provider = fallback_provider or HeuristicReasoningProvider()
        self.context_builder = context_builder or ReasoningContextBuilder()
        self.prompt_engine = prompt_engine or PromptEngine()
        self.enabled = enabled
        normalized_mode = mode or "assistive"
        self.mode = normalized_mode.strip().lower()
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
        """Evaluate a signal with a reasoning provider and return the result.

        The returned tuple contains the reasoning result (or None when disabled)
        and the context that was used to build the prompt.
        """
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
        """Determine whether a reasoning result should allow execution."""
        if self.mode == "assistive":
            return True

        if str(result.decision or "").strip().upper() == "REJECT":
            return False

        confidence = float(result.confidence or 0.0)
        return confidence >= self.minimum_confidence
