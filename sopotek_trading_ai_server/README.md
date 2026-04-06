# Sopotek Trading AI Server

This folder contains the full web-platform stack that was added for the SaaS and control-plane side of Sopotek Trading AI.

## Structure

- `backend/`: FastAPI API, JWT auth, WebSockets, Kafka bridge, async SQLAlchemy models
- `frontend/`: Next.js trading dashboard
- `kafka/`: topic definitions
- `docker/`: deployment files and environment example
- `docs/`: platform architecture notes

## Quick Start

```powershell
cd sopotek_trading_ai_server\backend
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

```powershell
cd sopotek_trading_ai_server\frontend
npm install
npm run dev
```

```powershell
cd sopotek_trading_ai_server\docker
docker compose -f docker-compose.platform.yml up --build
```
