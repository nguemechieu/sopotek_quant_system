from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Float, Integer, String, Text, select

from storage import database as storage_db


class Trade(storage_db.Base):
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
    def save_trade(self, symbol, side, quantity, price, exchange=None, order_id=None, order_type=None, status=None, timestamp=None, source=None, pnl=None, strategy_name=None, reason=None, confidence=None, expected_price=None, spread_bps=None, slippage_bps=None, fee=None, stop_loss=None, take_profit=None, setup=None, outcome=None, lessons=None, timeframe=None, signal_source_agent=None, consensus_status=None, adaptive_weight=None, adaptive_score=None):
        trade = Trade(
            exchange=str(exchange or "").lower() or None,
            order_id=str(order_id) if order_id is not None else None,
            symbol=str(symbol),
            side=str(side),
            source=str(source) if source is not None else None,
            order_type=str(order_type) if order_type is not None else None,
            status=str(status) if status is not None else None,
            quantity=float(quantity),
            price=float(price),
            pnl=self._normalize_float(pnl),
            strategy_name=str(strategy_name) if strategy_name is not None else None,
            reason=str(reason) if reason is not None else None,
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
        normalized_exchange = str(exchange or "").lower() or None
        normalized_order_id = str(order_id) if order_id is not None else None

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
            trade.symbol = str(symbol)
            trade.side = str(side)
            trade.source = str(source) if source is not None else None
            trade.order_type = str(order_type) if order_type is not None else None
            trade.status = str(status) if status is not None else None
            trade.quantity = float(quantity)
            trade.price = float(price)
            trade.pnl = self._normalize_float(pnl)
            trade.strategy_name = str(strategy_name) if strategy_name is not None else None
            trade.reason = str(reason) if reason is not None else None
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
        normalized_exchange = str(exchange or "").lower() or None
        normalized_order_id = str(order_id) if order_id is not None else None

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
            text_value = text_value[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text_value)
        except ValueError:
            return datetime.now(timezone.utc).replace(tzinfo=None)

        if parsed.tzinfo is None:
            return parsed
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)

    def _normalize_float(self, value):
        if value in (None, ""):
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _normalize_text(self, value):
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def get_trades(self, limit=200):
        with storage_db.SessionLocal() as session:
            stmt = select(Trade).order_by(Trade.timestamp.desc(), Trade.id.desc()).limit(int(limit))
            return list(session.execute(stmt).scalars().all())

    def get_by_symbol(self, symbol, limit=200):
        with storage_db.SessionLocal() as session:
            stmt = (
                select(Trade)
                .where(Trade.symbol == str(symbol))
                .order_by(Trade.timestamp.desc(), Trade.id.desc())
                .limit(int(limit))
            )
            return list(session.execute(stmt).scalars().all())
