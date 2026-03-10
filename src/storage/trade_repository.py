from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Float, Integer, String, select

from storage.database import Base, SessionLocal


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, index=True)
    exchange = Column(String, index=True)
    order_id = Column(String, index=True)
    symbol = Column(String, index=True)
    side = Column(String)
    order_type = Column(String)
    status = Column(String)
    quantity = Column(Float)
    price = Column(Float)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), index=True)


class TradeRepository:
    def save_trade(self, symbol, side, quantity, price, exchange=None, order_id=None, order_type=None, status=None, timestamp=None):
        trade = Trade(
            exchange=str(exchange or "").lower() or None,
            order_id=str(order_id) if order_id is not None else None,
            symbol=str(symbol),
            side=str(side),
            order_type=str(order_type) if order_type is not None else None,
            status=str(status) if status is not None else None,
            quantity=float(quantity),
            price=float(price),
            timestamp=self._normalize_timestamp(timestamp),
        )

        with SessionLocal() as session:
            session.add(trade)
            session.commit()
            session.refresh(trade)
            return trade

    def save_or_update_trade(self, symbol, side, quantity, price, exchange=None, order_id=None, order_type=None, status=None, timestamp=None):
        normalized_exchange = str(exchange or "").lower() or None
        normalized_order_id = str(order_id) if order_id is not None else None

        with SessionLocal() as session:
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
            trade.order_type = str(order_type) if order_type is not None else None
            trade.status = str(status) if status is not None else None
            trade.quantity = float(quantity)
            trade.price = float(price)
            trade.timestamp = self._normalize_timestamp(timestamp)

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

    def get_trades(self, limit=200):
        with SessionLocal() as session:
            stmt = select(Trade).order_by(Trade.timestamp.desc(), Trade.id.desc()).limit(int(limit))
            return list(session.execute(stmt).scalars().all())

    def get_by_symbol(self, symbol, limit=200):
        with SessionLocal() as session:
            stmt = (
                select(Trade)
                .where(Trade.symbol == str(symbol))
                .order_by(Trade.timestamp.desc(), Trade.id.desc())
                .limit(int(limit))
            )
            return list(session.execute(stmt).scalars().all())
