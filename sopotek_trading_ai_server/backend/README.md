# Sopotek Trading AI Platform Backend

Production-oriented FastAPI control plane for the Sopotek Trading AI web platform.

## What It Includes

- JWT authentication with `admin`, `trader`, and `viewer` roles
- Async SQLAlchemy models for `users`, `portfolios`, `trades`, `strategies`, and `logs`
- REST APIs for portfolio, positions, orders, strategies, risk, and trading controls
- WebSocket streams for market data, portfolio state, and execution updates
- Kafka integration layer with an in-memory fallback for local development and tests
- Docker support for PostgreSQL-backed deployment alongside the frontend and Kafka

## Local Run

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The backend uses `sqlite+aiosqlite` by default for local bootstrap and switches to PostgreSQL when `SOPOTEK_PLATFORM_DATABASE_URL` is set.
