# Release Notes

## Version 1.0.0

Release date: March 31, 2026

`1.0.0` is the first publishable desktop release of Sopotek Trading AI.

### Highlights

- desktop dashboard and terminal workflow for launching broker-backed or paper sessions
- MT4-style charting, detachable chart windows, order book views, depth views, and market info tabs
- manual trading ticket with broker-aware formatting, safety-aware sizing, and order feedback
- AI-assisted workflows including recommendations, Sopotek Pilot, and review-oriented runtime summaries
- Telegram remote console with menu-driven navigation, screenshots, chart captures, and confirmation-gated controls
- journaling, trade review, performance tooling, backtesting, and strategy optimization workflows
- runtime translation support for static UI labels plus dynamic summaries and rich-text views

### Release Readiness Notes

- prefer `paper`, `practice`, or `sandbox` validation before any meaningful live capital use
- validate broker login, candles, balances, positions, and manual order flow before enabling AI trading
- validate Telegram, OpenAI, and screenshot workflows only after the core trading path is healthy
- use `requirements.txt` for the full desktop runtime even though `pyproject.toml` now contains first-release package metadata

### Known Operational Constraints

- some broker capabilities remain venue-specific, so not every symbol or order type is supported across every adapter
- GUI, Telegram, OpenAI, and voice features are environment-sensitive and should be smoke-tested in the actual operator environment
- live execution remains powerful but high risk, so operator review, behavior guard, and kill-switch controls should stay part of the normal workflow

### First Release Focus

This first published version is focused on shipping a coherent operator workstation rather than maximizing breadth everywhere at once. The release priorities were:

- stable desktop startup and shutdown behavior
- realistic chart, order, and trade supervision workflows
- a usable Telegram remote console
- contributor-facing and operator-facing documentation that can support onboarding
