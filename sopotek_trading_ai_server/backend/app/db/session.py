from __future__ import annotations

from collections.abc import AsyncIterator
import logging

from sqlalchemy import event, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.base import Base, TimestampedMixin, coerce_utc_datetime


TIMESTAMPED_TABLES = ("users", "portfolios", "strategies", "trades", "logs", "workspace_configs")
TIMESTAMPED_COLUMNS = ("created_at", "updated_at")
logger = logging.getLogger(__name__)


def create_session_factory(settings: Settings) -> tuple[object, async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(settings.database_url, echo=False, future=True)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
    return engine, session_factory


def _engine_timestamp_overrides(bind: Connection | Engine | None) -> dict[str, dict[str, bool]]:
    if bind is None:
        return {}
    if isinstance(bind, Connection):
        return dict(getattr(bind.engine, "_sopotek_timestamp_timezone_overrides", {}) or {})
    return dict(getattr(bind, "_sopotek_timestamp_timezone_overrides", {}) or {})


def _timestamp_timezone_flags(
    obj: TimestampedMixin,
    *,
    timezone_overrides: dict[str, dict[str, bool]] | None = None,
) -> tuple[bool, bool]:
    overrides = dict(timezone_overrides or {})
    table_name = str(getattr(obj, "__tablename__", "") or "").strip()
    table_overrides = dict(overrides.get(table_name, {}) or {})
    created_at_column = obj.__table__.c.created_at
    updated_at_column = obj.__table__.c.updated_at
    created_at_timezone = bool(
        table_overrides.get("created_at", getattr(created_at_column.type, "timezone", False))
    )
    updated_at_timezone = bool(
        table_overrides.get("updated_at", getattr(updated_at_column.type, "timezone", False))
    )
    return created_at_timezone, updated_at_timezone


def normalize_timestamped_model(
    obj: object,
    *,
    timezone_overrides: dict[str, dict[str, bool]] | None = None,
) -> None:
    if not isinstance(obj, TimestampedMixin):
        return

    created_at_timezone, updated_at_timezone = _timestamp_timezone_flags(
        obj,
        timezone_overrides=timezone_overrides,
    )

    setattr(
        obj,
        "created_at",
        coerce_utc_datetime(getattr(obj, "created_at", None), timezone_aware=created_at_timezone),
    )
    setattr(
        obj,
        "updated_at",
        coerce_utc_datetime(getattr(obj, "updated_at", None), timezone_aware=updated_at_timezone),
    )


@event.listens_for(Session, "before_flush")
def normalize_timestamped_models_before_flush(session: Session, flush_context, instances) -> None:
    timezone_overrides = _engine_timestamp_overrides(session.get_bind())
    for obj in list(session.new) + list(session.dirty):
        normalize_timestamped_model(obj, timezone_overrides=timezone_overrides)


async def inspect_postgres_timestamp_columns(connection: AsyncConnection) -> dict[str, dict[str, bool]]:
    quoted_tables = ", ".join(f"'{table_name}'" for table_name in TIMESTAMPED_TABLES)
    quoted_columns = ", ".join(f"'{column_name}'" for column_name in TIMESTAMPED_COLUMNS)
    result = await connection.execute(
        text(
            f"""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name IN ({quoted_tables})
              AND column_name IN ({quoted_columns})
            """
        )
    )
    overrides: dict[str, dict[str, bool]] = {}
    for row in result.mappings():
        overrides.setdefault(str(row["table_name"]), {})[str(row["column_name"])] = (
            str(row["data_type"]).strip().lower() == "timestamp with time zone"
        )
    return overrides


def apply_timestamp_timezone_overrides(engine: object, overrides: dict[str, dict[str, bool]]) -> None:
    sync_engine = getattr(engine, "sync_engine", engine)
    setattr(sync_engine, "_sopotek_timestamp_timezone_overrides", dict(overrides or {}))


async def repair_postgres_timestamp_columns(connection: AsyncConnection) -> None:
    quoted_tables = ", ".join(f"'{table_name}'" for table_name in TIMESTAMPED_TABLES)
    quoted_columns = ", ".join(f"'{column_name}'" for column_name in TIMESTAMPED_COLUMNS)
    await connection.execute(
        text(
            f"""
            DO $$
            DECLARE
                target RECORD;
            BEGIN
                FOR target IN
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name IN ({quoted_tables})
                      AND column_name IN ({quoted_columns})
                      AND data_type = 'timestamp without time zone'
                LOOP
                    EXECUTE format(
                        'ALTER TABLE %I ALTER COLUMN %I TYPE TIMESTAMP WITH TIME ZONE USING %I AT TIME ZONE ''UTC''',
                        target.table_name,
                        target.column_name,
                        target.column_name
                    );
                END LOOP;
            END $$;
            """
        )
    )


async def init_db(engine: object) -> None:
    async_engine = engine
    from app.models import log, portfolio, strategy, trade, user, workspace_config  # noqa: F401

    logger.info(
        "ORM timestamp mode users.created_at timezone=%s",
        getattr(user.User.__table__.c.created_at.type, "timezone", None),
    )
    async with async_engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        if async_engine.dialect.name == "postgresql":
            await repair_postgres_timestamp_columns(connection)
            apply_timestamp_timezone_overrides(
                async_engine,
                await inspect_postgres_timestamp_columns(connection),
            )
        else:
            apply_timestamp_timezone_overrides(async_engine, {})


async def get_db_session(session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session
