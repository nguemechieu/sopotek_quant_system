import os
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import declarative_base, sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_DATABASE_URL = f"sqlite:///{(DATA_DIR / 'sopotek_trading.db').as_posix()}"
DATABASE_URL = os.getenv("SOPOTEK_DATABASE_URL", DEFAULT_DATABASE_URL)

engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    expire_on_commit=False,
    future=True,
)

Base = declarative_base()


def _table_columns(table_name):
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def _ensure_sqlite_column(table_name, column_name, ddl):
    if not DATABASE_URL.startswith("sqlite"):
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


def init_database():
    # Import models before create_all so SQLAlchemy sees the mapped tables.
    from storage import market_data_repository  # noqa: F401
    from storage import trade_repository  # noqa: F401

    Base.metadata.create_all(bind=engine)
    _migrate_sqlite_schema()

