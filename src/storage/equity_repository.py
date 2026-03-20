import json
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Float, Integer, String, Text, select

from storage import database as storage_db


class EquitySnapshot(storage_db.Base):
    __tablename__ = "equity_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    exchange = Column(String, index=True)
    account_label = Column(String, index=True)
    equity = Column(Float, index=True)
    balance = Column(Float)
    free_margin = Column(Float)
    used_margin = Column(Float)
    payload_json = Column(Text)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), index=True)


class EquitySnapshotRepository:
    def save_snapshot(
        self,
        equity,
        exchange=None,
        account_label=None,
        timestamp=None,
        balance=None,
        free_margin=None,
        used_margin=None,
        payload=None,
    ):
        snapshot = EquitySnapshot(
            exchange=str(exchange or "").lower() or None,
            account_label=str(account_label or "").strip() or None,
            equity=float(equity),
            balance=self._normalize_float(balance),
            free_margin=self._normalize_float(free_margin),
            used_margin=self._normalize_float(used_margin),
            payload_json=self._normalize_payload(payload),
            timestamp=self._normalize_timestamp(timestamp),
        )

        with storage_db.SessionLocal() as session:
            session.add(snapshot)
            session.commit()
            session.refresh(snapshot)
            return snapshot

    def get_snapshots(self, limit=2000, exchange=None, account_label=None):
        with storage_db.SessionLocal() as session:
            stmt = select(EquitySnapshot)
            normalized_exchange = str(exchange or "").lower() or None
            normalized_account = str(account_label or "").strip() or None
            if normalized_exchange:
                stmt = stmt.where(EquitySnapshot.exchange == normalized_exchange)
            if normalized_account:
                stmt = stmt.where(EquitySnapshot.account_label == normalized_account)
            stmt = stmt.order_by(EquitySnapshot.timestamp.desc(), EquitySnapshot.id.desc()).limit(int(limit))
            return list(session.execute(stmt).scalars().all())

    def latest_snapshot(self, exchange=None, account_label=None):
        rows = self.get_snapshots(limit=1, exchange=exchange, account_label=account_label)
        return rows[0] if rows else None

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

    def _normalize_payload(self, payload):
        if payload in (None, ""):
            return None
        try:
            return json.dumps(payload, default=str)
        except Exception:
            return json.dumps({"value": str(payload)})
