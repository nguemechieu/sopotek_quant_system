from fastapi import APIRouter

from app.api.routes import auth, control, orders, portfolio, positions, risk, strategies, workspace


api_router = APIRouter()
api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(portfolio.router, prefix="/portfolio", tags=["portfolio"])
api_router.include_router(positions.router, prefix="/positions", tags=["positions"])
api_router.include_router(orders.router, prefix="/orders", tags=["orders"])
api_router.include_router(strategies.router, prefix="/strategies", tags=["strategies"])
api_router.include_router(risk.router, prefix="/risk", tags=["risk"])
api_router.include_router(control.router, prefix="/control", tags=["control"])
api_router.include_router(workspace.router, prefix="/workspace", tags=["workspace"])
