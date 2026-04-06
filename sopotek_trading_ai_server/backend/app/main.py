from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.core.config import Settings, get_settings
from app.db.session import create_session_factory, init_db
from app.services.bootstrap import ensure_bootstrap_admin
from app.services.command_service import TradingControlService
from app.services.core_bridge import TradingCoreBridge
from app.services.kafka_gateway import build_kafka_gateway
from app.services.state_store import PlatformStateStore
from app.ws.router import router as websocket_router


def create_app(settings: Settings | None = None) -> FastAPI:
    active_settings = settings or get_settings()
    engine, session_factory = create_session_factory(active_settings)
    platform_state = PlatformStateStore()
    kafka_gateway = build_kafka_gateway(active_settings)
    control_service = TradingControlService(
        settings=active_settings,
        state_store=platform_state,
        kafka_gateway=kafka_gateway,
    )
    core_bridge = TradingCoreBridge(
        settings=active_settings,
        state_store=platform_state,
        kafka_gateway=kafka_gateway,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = active_settings
        app.state.engine = engine
        app.state.session_factory = session_factory
        app.state.platform_state = platform_state
        app.state.kafka_gateway = kafka_gateway
        app.state.control_service = control_service

        await init_db(engine)
        async with session_factory() as session:
            await ensure_bootstrap_admin(session, active_settings)
            await session.commit()

        core_bridge.bind()
        await kafka_gateway.start()
        try:
            yield
        finally:
            await kafka_gateway.stop()
            await engine.dispose()

    app = FastAPI(
        title=active_settings.app_name,
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(active_settings.cors_origins),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/healthz")
    async def healthcheck() -> dict[str, str]:
        return {"status": "ok", "environment": active_settings.environment}

    app.include_router(api_router)
    app.include_router(websocket_router)
    return app


app = create_app()
