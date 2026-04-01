from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Float, Integer, String, Text, select

from storage import database as storage_db


class Trade(storage_db.Base):
    """ORM model for persisted trades."""

    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, index=True)
    exchange = Column(String, index=True)
    order_id = Column(String, index=True)
    symbol = Column(String, index=True)
    side = Column(String)
    source = Column(String)
    order_type = Column(String)
    status = Column(String)
    quantity = Column(Float)
    price = Column(Float)
    pnl = Column(Float)
    strategy_name = Column(String)
    reason = Column(String)
    confidence = Column(Float)
    expected_price = Column(Float)
    spread_bps = Column(Float)
    slippage_bps = Column(Float)
    fee = Column(Float)
    stop_loss = Column(Float)
    take_profit = Column(Float)
    setup = Column(Text)
    outcome = Column(Text)
    lessons = Column(Text)
    timeframe = Column(String)
    signal_source_agent = Column(String)
    consensus_status = Column(String)
    adaptive_weight = Column(Float)
    adaptive_score = Column(Float)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), index=True)


class TradeRepository:
    """Repository for storing and querying trade records."""

    def save_trade(self, symbol, side, quantity, price, exchange=None, order_id=None, order_type=None, status=None, timestamp=None, source=None, pnl=None, strategy_name=None, reason=None, confidence=None, expected_price=None, spread_bps=None, slippage_bps=None, fee=None, stop_loss=None, take_profit=None, setup=None, outcome=None, lessons=None, timeframe=None, signal_source_agent=None, consensus_status=None, adaptive_weight=None, adaptive_score=None):
        """Persist a new trade record into the database."""
        trade = Trade(
            exchange=self._normalize_exchange(exchange),
            order_id=self._normalize_text(order_id),
            symbol=self._normalize_text(symbol),
            side=self._normalize_text(side),
            source=self._normalize_text(source),
            order_type=self._normalize_text(order_type),
            status=self._normalize_text(status),
            quantity=self._normalize_float(quantity),
            price=self._normalize_float(price),
            pnl=self._normalize_float(pnl),
            strategy_name=self._normalize_text(strategy_name),
            reason=self._normalize_text(reason),
            confidence=self._normalize_float(confidence),
            expected_price=self._normalize_float(expected_price),
            spread_bps=self._normalize_float(spread_bps),
            slippage_bps=self._normalize_float(slippage_bps),
            fee=self._normalize_float(fee),
            stop_loss=self._normalize_float(stop_loss),
            take_profit=self._normalize_float(take_profit),
            setup=self._normalize_text(setup),
            outcome=self._normalize_text(outcome),
            lessons=self._normalize_text(lessons),
            timeframe=self._normalize_text(timeframe),
            signal_source_agent=self._normalize_text(signal_source_agent),
            consensus_status=self._normalize_text(consensus_status),
            adaptive_weight=self._normalize_float(adaptive_weight),
            adaptive_score=self._normalize_float(adaptive_score),
            timestamp=self._normalize_timestamp(timestamp),
        )

        with storage_db.SessionLocal() as session:
            session.add(trade)
            session.commit()
            session.refresh(trade)
            return trade

    def save_or_update_trade(self, symbol, side, quantity, price, exchange=None, order_id=None, order_type=None, status=None, timestamp=None, source=None, pnl=None, strategy_name=None, reason=None, confidence=None, expected_price=None, spread_bps=None, slippage_bps=None, fee=None, stop_loss=None, take_profit=None, setup=None, outcome=None, lessons=None, timeframe=None, signal_source_agent=None, consensus_status=None, adaptive_weight=None, adaptive_score=None):
        """Insert a trade record or update an existing trade with the same order ID."""
        normalized_exchange = self._normalize_exchange(exchange)
        normalized_order_id = self._normalize_text(order_id)

        with storage_db.SessionLocal() as session:
            trade = None
            if normalized_order_id:
                stmt = select(Trade).where(Trade.order_id == normalized_order_id)
                if normalized_exchange:
                    stmt = stmt.where(Trade.exchange == normalized_exchange)
                stmt = stmt.order_by(Trade.id.desc()).limit(1)
                trade = session.execute(stmt).scalars().first()

            if trade is None:
                trade = Trade()
                session.add(trade)

            trade.exchange = normalized_exchange
            trade.order_id = normalized_order_id
            trade.symbol = self._normalize_text(symbol)
            trade.side = self._normalize_text(side)
            trade.source = self._normalize_text(source)
            trade.order_type = self._normalize_text(order_type)
            trade.status = self._normalize_text(status)
            trade.quantity = self._normalize_float(quantity)
            trade.price = self._normalize_float(price)
            trade.pnl = self._normalize_float(pnl)
            trade.strategy_name = self._normalize_text(strategy_name)
            trade.reason = self._normalize_text(reason)
            trade.confidence = self._normalize_float(confidence)
            trade.expected_price = self._normalize_float(expected_price)
            trade.spread_bps = self._normalize_float(spread_bps)
            trade.slippage_bps = self._normalize_float(slippage_bps)
            trade.fee = self._normalize_float(fee)
            trade.stop_loss = self._normalize_float(stop_loss)
            trade.take_profit = self._normalize_float(take_profit)
            trade.setup = self._normalize_text(setup)
            trade.outcome = self._normalize_text(outcome)
            trade.lessons = self._normalize_text(lessons)
            trade.timeframe = self._normalize_text(timeframe)
            trade.signal_source_agent = self._normalize_text(signal_source_agent)
            trade.consensus_status = self._normalize_text(consensus_status)
            trade.adaptive_weight = self._normalize_float(adaptive_weight)
            trade.adaptive_score = self._normalize_float(adaptive_score)
            trade.timestamp = self._normalize_timestamp(timestamp)

            session.commit()
            session.refresh(trade)
            return trade

    def update_trade_journal(
        self,
        trade_id=None,
        order_id=None,
        exchange=None,
        reason=None,
        stop_loss=None,
        take_profit=None,
        setup=None,
        outcome=None,
        lessons=None,
    ):
        """Update journal fields for an existing trade record."""
        normalized_exchange = self._normalize_exchange(exchange)
        normalized_order_id = self._normalize_text(order_id)

        with storage_db.SessionLocal() as session:
            trade = None
            if trade_id is not None:
                trade = session.get(Trade, int(trade_id))
            if trade is None and normalized_order_id:
                stmt = select(Trade).where(Trade.order_id == normalized_order_id)
                if normalized_exchange:
                    stmt = stmt.where(Trade.exchange == normalized_exchange)
                stmt = stmt.order_by(Trade.id.desc()).limit(1)
                trade = session.execute(stmt).scalars().first()
            if trade is None:
                return None

            trade.reason = self._normalize_text(reason)
            trade.stop_loss = self._normalize_float(stop_loss)
            trade.take_profit = self._normalize_float(take_profit)
            trade.setup = self._normalize_text(setup)
            trade.outcome = self._normalize_text(outcome)
            trade.lessons = self._normalize_text(lessons)

            session.commit()
            session.refresh(trade)
            return trade

    def _normalize_timestamp(self, value):
        """Normalize timestamp values into UTC-naive datetime objects."""
        if value is None:
            return datetime.now(timezone.utc).replace(tzinfo=None)
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        try:
            numeric = float(value)
            if abs(numeric) > 1e11:
                return datetime.fromtimestamp(numeric / 1000.0, tz=timezone.utc).replace(tzinfo=None)
            return datetime.fromtimestamp(numeric, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            pass

        text_value = str(value).strip()
        if text_value.endswith("Z"):
            text_value = f"{text_value[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text_value)
        except ValueError:
            return datetime.now(timezone.utc).replace(tzinfo=None)

        if parsed.tzinfo is None:
            return parsed
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)

    def _normalize_float(self, value):
        """Convert a value to float, returning None for invalid inputs."""
        if value in (None, ""):
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _normalize_text(self, value):
        """Convert a value to trimmed text, returning None for empty strings."""
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _normalize_exchange(self, value):
        """Normalize exchange names to lowercase text."""
        normalized = self._normalize_text(value)
        if normalized is None:
            return None
        return normalized.lower()

    def get_trades(self, limit=200, exchange=None):
        """Return recent trades, optionally filtered by exchange."""
        normalized_exchange = self._normalize_exchange(exchange)
        with storage_db.SessionLocal() as session:
            stmt = select(Trade)
            if normalized_exchange:
                stmt = stmt.where(Trade.exchange == normalized_exchange)
            stmt = stmt.order_by(Trade.timestamp.desc(), Trade.id.desc()).limit(int(limit))
            return list(session.execute(stmt).scalars().all())

    def get_by_symbol(self, symbol, limit=200, exchange=None):
        """Return recent trades for a symbol, optionally filtered by exchange."""
        normalized_exchange = self._normalize_exchange(exchange)
        with storage_db.SessionLocal() as session:
            stmt = select(Trade).where(Trade.symbol == self._normalize_text(symbol))
            if normalized_exchange:
                stmt = stmt.where(Trade.exchange == normalized_exchange)
            stmt = stmt.order_by(Trade.timestamp.desc(), Trade.id.desc()).limit(int(limit))
            return list(session.execute(stmt).scalars().all())
