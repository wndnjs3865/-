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
| 3 | agents/mia, qr, cso, po, ima | DONE (29 tests) |
| 4 | exchanges/ (Binance, Bybit, Paper) | TODO |
| 5 | api/ (FastAPI + WebSocket) | TODO |

## Key Design Decisions
- MessageBus: correlation_id-based request-response resolves futures by topic match (not just correlation_id) to avoid request/response confusion
- AuditLogger: WAL mode SQLite + async executor writes, auto-rotation at max_entries/2
- CRCO: 11 risk checks, circuit breaker with cooldown, absolute veto authority
- ES: Paper mode uses simulated slippage (2bps) + commission (4bps on notional), trailing stop activates after TP1
- CSO: Pipeline runs in background task (`create_task`) to avoid MessageBus dispatcher deadlock
- MIA: Multi-source data gathering structure ready for real API integration (price, SMC, sentiment, macro, on-chain, psychology)
- QR: 8-dimension scoring with weighted total (trend 20%, momentum 15%, orderflow 15%, etc.)
- PO: Equity curve tracking, Sharpe ratio (annualized sqrt(252)), profit factor, max drawdown from peak
- IMA: Health collection from registered agents, alert severity-based Telegram routing

## Bugs Found & Fixed
- Phase 1: MessageBus `_deliver` resolved futures on request message instead of response (moved correlation check before subscriber filter + added topic matching)
- Phase 2: ES commission calculated on quantity instead of notional value (price * qty)
- Phase 2: Trailing stop pre-set trail_price during activation, causing first update to skip (removed pre-set, let update block handle initial value)
- Phase 3: **CSO deadlock** — `_handle_trade_signal` called `request()` (which awaits a Future) inside a MessageBus handler, blocking the dispatcher from processing the QR/CRCO responses. Fix: spawn pipeline as `create_task()` so handler returns immediately.

## Critical Pattern: Avoid await in MessageBus Handlers
**NEVER use `request()` or `publish_and_wait()` inside a `subscribe_safe()` handler.**
The dispatcher delivers messages sequentially per handler. If a handler awaits a response that must be delivered by the same dispatcher, it deadlocks. Solution: use `create_task()` to run long pipelines asynchronously.

## Agent Message Flow (Verified by Tests)
```
MIA → mia.trade_signal → CSO (spawns pipeline task)
  CSO → cso.request_quant_score → QR → qr.quant_score → CSO
  CSO → cso.request_risk_assessment → CRCO → crco.risk_assessment → CSO
  CSO → cso.trade_decision → ES
    ES → es.order_submitted, es.order_filled → PO, CRCO
    ES → es.order_closed → PO, CRCO
CRCO → crco.circuit_breaker → ALL (broadcast)
IMA ← ima.alert (from any agent)
```

## Naming Conventions
- Topics: `{agent_lower}.{action}` (e.g. `crco.risk_assessment`, `es.order_filled`)
- Models: PascalCase Pydantic models in core/models.py
- Agent classes: `{Role}Agent` (e.g. CRCOAgent, ESAgent, MIAAgent)
- Constructor: `__init__(self, bus, audit, config)` for all agents
- Config: dataclass-based, loaded from env vars

## Test Coverage
- Total: 74 tests passing (Phase 1: 29, Phase 2: 16, Phase 3: 29)
- Full integration: CSO pipeline test runs MIA→QR→CRCO→ES end-to-end
- All agents tested with real MessageBus + AuditLogger (no mocks for core infra)
- Critical paths: veto flow, circuit breaker, pipeline deadlock prevention, order lifecycle, trailing stop, performance metrics
