# QuantPulse v3 — Project Memory

## Architecture
- 7-agent multi-agent quant trading system (MIA, QR, CSO, CRCO, ES, PO, IMA)
- Cloud-native (Railway/Replit/Docker), no Termux/mobile dependency
- Async MessageBus (PubSub + request-response + priority queue)
- SQLite AuditLogger for every decision
- BaseAgent ABC with retry, health, error handling
- JWQuantSystem orchestrator with ordered boot/shutdown

## Implementation Status
| Phase | Contents | Status | Tests |
|-------|----------|--------|-------|
| 1 | core/models, message_bus, audit_logger, base_agent, config | DONE | 29 |
| 2 | agents/crco.py, agents/es.py | DONE | 16 |
| 3 | agents/mia, qr, cso, po, ima | DONE | 29 |
| 4 | main.py (JWQuantSystem), deployment files, integration tests | DONE | 6 |
| **Total** | **All 7 agents + core + orchestrator** | **DONE** | **80** |

## Key Design Decisions
- MessageBus: correlation_id-based request-response resolves futures by topic match (not just correlation_id)
- AuditLogger: WAL mode SQLite + async executor writes, auto-rotation at max_entries/2
- CRCO: 11 risk checks, circuit breaker with cooldown, absolute veto authority
- ES: Paper mode simulated slippage (2bps) + commission (4bps on notional), trailing stop after TP1
- CSO: Pipeline runs in background task (`create_task`) to avoid MessageBus dispatcher deadlock
- JWQuantSystem: Boot order = consumers first (IMA→PO→ES→CRCO→QR→CSO→MIA), shutdown in reverse
- MIA: Multi-source data pipeline ready for real API integration
- QR: 8-dimension weighted scoring (trend 20%, momentum 15%, orderflow 15%, etc.)

## Bugs Found & Fixed (All Phases)
1. **Phase 1**: MessageBus `_deliver` resolved futures on request message instead of response → added topic matching
2. **Phase 2**: ES commission on quantity instead of notional → fixed to `price * qty * rate`
3. **Phase 2**: Trailing stop pre-set trail_price during activation → removed pre-set
4. **Phase 3**: CSO deadlock in handler → `create_task()` for pipeline
5. **Phase 4**: No new bugs — clean integration

## Critical Pattern: Avoid await in MessageBus Handlers
**NEVER use `request()` or `publish_and_wait()` inside a `subscribe_safe()` handler.**
Use `create_task()` to run long pipelines asynchronously.

## Agent Message Flow (Verified End-to-End)
```
MIA → mia.trade_signal → CSO (spawns pipeline task)
  CSO → cso.request_quant_score → QR → qr.quant_score → CSO
  CSO → cso.request_risk_assessment → CRCO → crco.risk_assessment → CSO
  CSO → cso.trade_decision → ES
    ES → es.order_submitted, es.order_filled → PO, CRCO
    ES → es.order_closed → PO, CRCO
CRCO → crco.circuit_breaker → CSO, ES, MIA (broadcast)
IMA ← ima.alert (from any agent)
IMA → ima.health_report (periodic)
```

## Naming Conventions
- Topics: `{agent_lower}.{action}` (e.g. `crco.risk_assessment`)
- Agent classes: `{Role}Agent` (e.g. CRCOAgent, MIAAgent)
- Constructor: `__init__(self, bus, audit, config)` for all agents
- Config: dataclass-based, loaded from env vars

## File Structure
```
quantpulse_v3/
├── main.py              # JWQuantSystem orchestrator
├── config.py            # Env-based configuration
├── core/                # Infrastructure (models, bus, audit, base_agent)
├── agents/              # 7 agents (mia, qr, cso, crco, es, po, ima)
├── strategies/          # Phase 5+ (SMC/ICT engine)
├── exchanges/           # Phase 5+ (Binance, Bybit, Paper)
├── api/                 # Phase 5+ (FastAPI + WebSocket)
├── tests/               # 80 tests across 9 test files
├── scripts/start.sh     # Cloud deployment entrypoint
├── Dockerfile           # Container build
├── railway.toml         # Railway config
├── requirements.txt     # Pinned dependencies
└── CLAUDE.md            # This file (project memory)
```

## Competitive Analysis (Optimizer Agent)

### vs Trading Frameworks
| Feature | QuantPulse v3 | Freqtrade | Hummingbot | NautilusTrader |
|---------|--------------|-----------|------------|----------------|
| Multi-Agent Architecture | 7 specialized agents | Single bot | Single bot | Actor model |
| Risk Governance | CRCO with absolute veto + 11 checks | Basic stop-loss | Configurable | Risk engine |
| SMC/ICT Analysis | Native (MIA) | Plugin-based | No | No |
| Audit Trail | Every decision logged (SQLite) | Trade log | Trade log | Event log |
| Circuit Breaker | Cascade to all agents | No | No | Basic |
| Cloud-Native | Railway/Replit/Docker | Docker | Docker | Docker |

**QuantPulse Advantages**: Separation of concerns (analysis/risk/execution decoupled), CRCO veto cannot be bypassed by any other agent, full audit trail for compliance, multi-source intelligence pipeline.

**Areas for Improvement**: Real exchange integration (Phase 5), backtesting engine, web dashboard, strategy optimization via ML.

### vs Multi-Agent Frameworks
| Feature | QuantPulse v3 | MetaGPT | CrewAI | AutoGen |
|---------|--------------|---------|--------|---------|
| Domain | Quant Trading | Software Dev | General | General |
| Communication | Custom MessageBus (async) | SharedMemory | Task delegation | Chat |
| Governance | CRCO veto + AuditLog | SOP-based | Process-based | None |
| Real-time | Yes (async event loop) | No | No | No |
| Deterministic | Yes (rule-based) | LLM-dependent | LLM-dependent | LLM-dependent |

**QuantPulse Advantage**: Deterministic rule-based decisions (no LLM latency/cost in the hot path), real-time async processing, domain-specific risk governance. LLM can be layered on top for strategy adaptation in future phases.

## Test Coverage Summary
- **80 tests passing** across 9 test files
- Integration test: full system boot → signal pipeline → circuit breaker propagation
- Every agent tested with real MessageBus + AuditLogger (no mocks)
- Critical paths verified: veto, circuit breaker cascade, deadlock prevention, PnL computation
