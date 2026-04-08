# QuantPulse v3 — Project Memory

## Architecture
- 7-agent multi-agent quant trading system (MIA, QR, CSO, CRCO, ES, PO, IMA)
- Cloud-native (Railway/Replit), no Termux/mobile dependency
- Async MessageBus (PubSub + request-response + priority queue)
- SQLite AuditLogger for every decision
- BaseAgent ABC with retry, health, error handling

## Implementation Status
| Phase | Contents | Status |
|-------|----------|--------|
| 1 | core/models, message_bus, audit_logger, base_agent, config | DONE (29 tests) |
| 2 | agents/crco.py, agents/es.py | DONE (16 tests) |
| 3 | agents/mia, qr, cso, po, ima + strategies/ | TODO |
| 4 | exchanges/ (Binance, Bybit, Paper) | TODO |
| 5 | api/ (FastAPI + WebSocket) | TODO |

## Key Design Decisions
- MessageBus: correlation_id-based request-response resolves futures by topic match (not just correlation_id) to avoid request/response confusion
- AuditLogger: WAL mode SQLite + async executor writes, auto-rotation at max_entries/2
- CRCO: 11 risk checks, circuit breaker with cooldown, absolute veto authority
- ES: Paper mode uses simulated slippage (2bps) + commission (4bps on notional), trailing stop activates after TP1

## Bugs Found & Fixed
- Phase 1: MessageBus `_deliver` resolved futures on request message instead of response (moved correlation check before subscriber filter + added topic matching)
- Phase 2: ES commission calculated on quantity instead of notional value (price * qty)
- Phase 2: Trailing stop pre-set trail_price during activation, causing first update to skip (removed pre-set, let update block handle initial value)

## Naming Conventions
- Topics: `{agent_lower}.{action}` (e.g. `crco.risk_assessment`, `es.order_filled`)
- Models: PascalCase Pydantic models in core/models.py
- Agent classes: `{Role}Agent` (e.g. CRCOAgent, ESAgent)
- Config: dataclass-based, loaded from env vars

## Test Coverage
- Total: 45 tests passing
- All agents tested with real MessageBus + AuditLogger (no mocks for core infra)
- Critical paths: veto flow, circuit breaker, order lifecycle, trailing stop
