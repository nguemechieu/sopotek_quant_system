import os
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import declarative_base, sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_DATABASE_URL = f"sqlite:///{(DATA_DIR / 'sopotek_trading.db').as_posix()}"


def normalize_database_url(database_url=None):
    text = str(database_url or "").strip()
    return text or DEFAULT_DATABASE_URL


def is_sqlite_url(database_url=None):
    return normalize_database_url(database_url or DATABASE_URL).startswith("sqlite")


def _create_engine(database_url):
    normalized = normalize_database_url(database_url)
    return create_engine(
        normalized,
        echo=False,
        future=True,
        connect_args={"check_same_thread": False} if is_sqlite_url(normalized) else {},
    )


def _create_session_factory(active_engine):
    return sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=active_engine,
        expire_on_commit=False,
        future=True,
    )


DATABASE_URL = normalize_database_url(os.getenv("SOPOTEK_DATABASE_URL", DEFAULT_DATABASE_URL))
engine = _create_engine(DATABASE_URL)
SessionLocal = _create_session_factory(engine)

Base = declarative_base()


def _table_columns(table_name):
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def _ensure_sqlite_column(table_name, column_name, ddl):
    if not is_sqlite_url():
        return

    existing = _table_columns(table_name)
    if column_name in existing:
        return

    with engine.begin() as connection:
        connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {ddl}"))


def _migrate_sqlite_schema():
    # Existing local DBs may come from the earlier lightweight schema.
    _ensure_sqlite_column("candles", "exchange", "exchange VARCHAR")
    _ensure_sqlite_column("candles", "timeframe", "timeframe VARCHAR")
    _ensure_sqlite_column("candles", "timestamp_ms", "timestamp_ms BIGINT")

    _ensure_sqlite_column("trades", "exchange", "exchange VARCHAR")
    _ensure_sqlite_column("trades", "order_id", "order_id VARCHAR")
    _ensure_sqlite_column("trades", "order_type", "order_type VARCHAR")
    _ensure_sqlite_column("trades", "status", "status VARCHAR")
    _ensure_sqlite_column("trades", "source", "source VARCHAR")
    _ensure_sqlite_column("trades", "pnl", "pnl FLOAT")
    _ensure_sqlite_column("trades", "strategy_name", "strategy_name VARCHAR")
    _ensure_sqlite_column("trades", "reason", "reason VARCHAR")
    _ensure_sqlite_column("trades", "confidence", "confidence FLOAT")
    _ensure_sqlite_column("trades", "expected_price", "expected_price FLOAT")
    _ensure_sqlite_column("trades", "spread_bps", "spread_bps FLOAT")
    _ensure_sqlite_column("trades", "slippage_bps", "slippage_bps FLOAT")
    _ensure_sqlite_column("trades", "fee", "fee FLOAT")
    _ensure_sqlite_column("trades", "stop_loss", "stop_loss FLOAT")
    _ensure_sqlite_column("trades", "take_profit", "take_profit FLOAT")
    _ensure_sqlite_column("trades", "setup", "setup TEXT")
    _ensure_sqlite_column("trades", "outcome", "outcome TEXT")
    _ensure_sqlite_column("trades", "lessons", "lessons TEXT")
    _ensure_sqlite_column("trades", "timeframe", "timeframe VARCHAR")
    _ensure_sqlite_column("trades", "signal_source_agent", "signal_source_agent VARCHAR")
    _ensure_sqlite_column("trades", "consensus_status", "consensus_status VARCHAR")
    _ensure_sqlite_column("trades", "adaptive_weight", "adaptive_weight FLOAT")
    _ensure_sqlite_column("trades", "adaptive_score", "adaptive_score FLOAT")

    _ensure_sqlite_column("equity_snapshots", "exchange", "exchange VARCHAR")
    _ensure_sqlite_column("equity_snapshots", "account_label", "account_label VARCHAR")
    _ensure_sqlite_column("equity_snapshots", "equity", "equity FLOAT")
    _ensure_sqlite_column("equity_snapshots", "balance", "balance FLOAT")
    _ensure_sqlite_column("equity_snapshots", "free_margin", "free_margin FLOAT")
    _ensure_sqlite_column("equity_snapshots", "used_margin", "used_margin FLOAT")
    _ensure_sqlite_column("equity_snapshots", "payload_json", "payload_json TEXT")
    _ensure_sqlite_column("equity_snapshots", "timestamp", "timestamp DATETIME")

    _ensure_sqlite_column("agent_decisions", "decision_id", "decision_id VARCHAR")
    _ensure_sqlite_column("agent_decisions", "exchange", "exchange VARCHAR")
    _ensure_sqlite_column("agent_decisions", "account_label", "account_label VARCHAR")
    _ensure_sqlite_column("agent_decisions", "symbol", "symbol VARCHAR")
    _ensure_sqlite_column("agent_decisions", "agent_name", "agent_name VARCHAR")
    _ensure_sqlite_column("agent_decisions", "stage", "stage VARCHAR")
    _ensure_sqlite_column("agent_decisions", "strategy_name", "strategy_name VARCHAR")
    _ensure_sqlite_column("agent_decisions", "timeframe", "timeframe VARCHAR")
    _ensure_sqlite_column("agent_decisions", "side", "side VARCHAR")
    _ensure_sqlite_column("agent_decisions", "confidence", "confidence FLOAT")
    _ensure_sqlite_column("agent_decisions", "approved", "approved INTEGER")
    _ensure_sqlite_column("agent_decisions", "reason", "reason VARCHAR")
    _ensure_sqlite_column("agent_decisions", "payload_json", "payload_json TEXT")
    _ensure_sqlite_column("agent_decisions", "timestamp", "timestamp DATETIME")


def init_database():
    # Import models before create_all so SQLAlchemy sees the mapped tables.
    from storage import agent_decision_repository  # noqa: F401
    from storage import equity_repository  # noqa: F401
    from storage import market_data_repository  # noqa: F401
    from storage import paper_trade_learning_repository  # noqa: F401
    from storage import trade_audit_repository  # noqa: F401
    from storage import trade_repository  # noqa: F401

    Base.metadata.create_all(bind=engine)
    _migrate_sqlite_schema()


def get_database_url():
    return str(DATABASE_URL or DEFAULT_DATABASE_URL)


def configure_database(database_url=None):
    global DATABASE_URL, engine, SessionLocal

    normalized = normalize_database_url(database_url)
    current = normalize_database_url(DATABASE_URL)
    if normalized == current:
        return get_database_url()

    previous_engine = engine
    engine = _create_engine(normalized)
    SessionLocal = _create_session_factory(engine)
    DATABASE_URL = normalized
    os.environ["SOPOTEK_DATABASE_URL"] = normalized

    try:
        previous_engine.dispose()
    except Exception:
        pass

    return get_database_url()
