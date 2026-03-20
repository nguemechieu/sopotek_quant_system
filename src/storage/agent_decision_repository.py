import json
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Float, Integer, String, Text, select

from storage import database as storage_db


class AgentDecision(storage_db.Base):
    __tablename__ = "agent_decisions"

    id = Column(Integer, primary_key=True, index=True)
    decision_id = Column(String, index=True)
    exchange = Column(String, index=True)
    account_label = Column(String, index=True)
    symbol = Column(String, index=True)
    agent_name = Column(String, index=True)
    stage = Column(String, index=True)
    strategy_name = Column(String, index=True)
    timeframe = Column(String, index=True)
    side = Column(String)
    confidence = Column(Float)
    approved = Column(Integer)
    reason = Column(String)
    payload_json = Column(Text)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), index=True)


class AgentDecisionRepository:
    def save_decision(
        self,
        agent_name,
        stage,
        symbol=None,
        decision_id=None,
        exchange=None,
        account_label=None,
        strategy_name=None,
        timeframe=None,
        side=None,
        confidence=None,
        approved=None,
        reason=None,
        payload=None,
        timestamp=None,
    ):
        row = AgentDecision(
            decision_id=str(decision_id or "").strip() or None,
            exchange=str(exchange or "").lower() or None,
            account_label=str(account_label or "").strip() or None,
            symbol=str(symbol or "").strip().upper() or None,
            agent_name=str(agent_name or "").strip() or None,
            stage=str(stage or "").strip() or None,
            strategy_name=str(strategy_name or "").strip() or None,
            timeframe=str(timeframe or "").strip() or None,
            side=str(side or "").strip().lower() or None,
            confidence=self._normalize_float(confidence),
            approved=self._normalize_bool(approved),
            reason=self._normalize_text(reason),
            payload_json=self._normalize_payload(payload),
            timestamp=self._normalize_timestamp(timestamp),
        )

        with storage_db.SessionLocal() as session:
            session.add(row)
            session.commit()
            session.refresh(row)
            return row

    def get_decisions(self, limit=200, symbol=None, decision_id=None, exchange=None, account_label=None):
        with storage_db.SessionLocal() as session:
            stmt = select(AgentDecision)
            normalized_symbol = str(symbol or "").strip().upper() or None
            normalized_decision_id = str(decision_id or "").strip() or None
            normalized_exchange = str(exchange or "").lower() or None
            normalized_account = str(account_label or "").strip() or None
            if normalized_symbol:
                stmt = stmt.where(AgentDecision.symbol == normalized_symbol)
            if normalized_decision_id:
                stmt = stmt.where(AgentDecision.decision_id == normalized_decision_id)
            if normalized_exchange:
                stmt = stmt.where(AgentDecision.exchange == normalized_exchange)
            if normalized_account:
                stmt = stmt.where(AgentDecision.account_label == normalized_account)
            stmt = stmt.order_by(AgentDecision.timestamp.desc(), AgentDecision.id.desc()).limit(int(limit))
            return list(session.execute(stmt).scalars().all())

    def latest_chain_for_symbol(self, symbol, limit=50, exchange=None, account_label=None):
        rows = self.get_decisions(limit=max(int(limit) * 4, 100), symbol=symbol, exchange=exchange, account_label=account_label)
        if not rows:
            return []
        latest_decision_id = next((str(getattr(row, "decision_id", "") or "").strip() for row in rows if str(getattr(row, "decision_id", "") or "").strip()), "")
        if latest_decision_id:
            chain = [row for row in rows if str(getattr(row, "decision_id", "") or "").strip() == latest_decision_id]
        else:
            newest_symbol = str(getattr(rows[0], "symbol", "") or "").strip().upper()
            chain = [row for row in rows if str(getattr(row, "symbol", "") or "").strip().upper() == newest_symbol]
        chain = list(reversed(chain))
        return chain[-max(1, int(limit)):]

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

    def _normalize_bool(self, value):
        if value in (None, ""):
            return None
        return 1 if bool(value) else 0

    def _normalize_text(self, value):
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _normalize_payload(self, payload):
        if payload in (None, ""):
            return None
        try:
            return json.dumps(payload, default=str)
        except Exception:
            return json.dumps({"value": str(payload)})
