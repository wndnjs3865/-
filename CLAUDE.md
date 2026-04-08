# QuantPulse v3 — Project Memory (Final)

## Architecture
- 7-agent multi-agent quant trading system (MIA, QR, CSO, CRCO, ES, PO, IMA)
- Cloud-native (Railway/Replit/Docker), no Termux/mobile dependency
- Async MessageBus (PubSub + request-response + priority queue)
- SQLite AuditLogger for every decision
- BaseAgent ABC with retry, health, error handling
- JWQuantSystem orchestrator with ordered boot/shutdown
- PaperExchange with CCXT-compatible interface

## Implementation Status — COMPLETE
| Phase | Contents | Tests |
|-------|----------|-------|
| 1 | core/ (models, message_bus, audit_logger, base_agent, config) | 29 |
| 2 | agents/crco.py, agents/es.py | 16 |
| 3 | agents/mia, qr, cso, po, ima | 29 |
| 4 | main.py (JWQuantSystem), deployment, integration | 6 |
| Final | exchanges/, price feed wiring, README, Docker | 6 |
| **Total** | **Complete system** | **86** |

## Key Design Decisions
1. **MessageBus**: correlation_id + topic matching for request-response (avoids request/response confusion)
2. **AuditLogger**: WAL mode SQLite + async executor, auto-rotation at max_entries/2
3. **CRCO**: 11 risk checks, circuit breaker with cooldown, absolute veto
4. **ES**: Simulated slippage (2bps) + commission (4bps on notional), trailing stop after TP1, price feed injection for SL/TP monitoring
5. **CSO**: Pipeline in `create_task()` to prevent dispatcher deadlock
6. **Boot order**: Consumers first (IMA→PO→ES→CRCO→QR→CSO→MIA), shutdown in reverse
7. **PaperExchange**: CCXT-compatible interface, ready for live connector swap

## Bugs Found & Fixed (All Phases)
1. MessageBus future resolved on request instead of response → topic matching
2. ES commission on quantity instead of notional → `price * qty * rate`
3. Trailing stop pre-set trail_price → removed, let update handle first value
4. CSO deadlock in handler → `create_task()` for pipeline
5. ES `_check_sl_tp` was a no-op → wired to price feed with full SL/TP/trailing logic

## Critical Pattern
**NEVER use `request()` or `publish_and_wait()` inside a `subscribe_safe()` handler.** Use `create_task()` to run long pipelines.

## Message Flow (Verified E2E)
```
MIA → mia.trade_signal → CSO (pipeline task)
  CSO → cso.request_quant_score → QR → qr.quant_score → CSO
  CSO → cso.request_risk_assessment → CRCO → crco.risk_assessment → CSO
  CSO → cso.trade_decision → ES
    ES → es.order_submitted/filled → PO, CRCO
    ES → es.order_closed → PO, CRCO
CRCO → crco.circuit_breaker → CSO, ES, MIA (broadcast)
IMA ← ima.alert (from any agent)
```

## Competitive Analysis

### vs Trading Frameworks
| Feature | QuantPulse v3 | Freqtrade | Hummingbot | NautilusTrader |
|---------|--------------|-----------|------------|----------------|
| Multi-Agent | 7 specialized | Single bot | Single bot | Actor model |
| Risk Governance | CRCO absolute veto + 11 checks | Basic SL | Configurable | Risk engine |
| SMC/ICT | Native (MIA) | Plugin | No | No |
| Audit Trail | Every decision (SQLite) | Trade log | Trade log | Event log |
| Circuit Breaker | Cascade all agents | No | No | Basic |

### vs Multi-Agent Frameworks
| Feature | QuantPulse v3 | MetaGPT | CrewAI | AutoGen |
|---------|--------------|---------|--------|---------|
| Domain | Quant Trading | Software Dev | General | General |
| Communication | Custom async MessageBus | SharedMemory | Delegation | Chat |
| Governance | CRCO veto + AuditLog | SOP | Process | None |
| Real-time | Yes (async loop) | No | No | No |
| Deterministic | Yes (rule-based) | LLM-dependent | LLM-dependent | LLM-dependent |

**QuantPulse Advantage**: Deterministic, real-time, domain-optimized, full audit, absolute risk governance. No LLM in the hot path = no latency/cost/hallucination risk.

## File Structure (Final)
```
quantpulse_v3/
├── main.py              # JWQuantSystem orchestrator
├── config.py            # Env-based configuration
├── core/                # Infrastructure (models, bus, audit, base_agent)
├── agents/              # 7 agents (mia, qr, cso, crco, es, po, ima)
├── exchanges/           # BaseExchange + PaperExchange
├── tests/               # 86 tests (10 test files)
├── scripts/             # start.sh, healthcheck.sh
├── Dockerfile           # Optimized container
├── docker-compose.yml   # Single-command deployment
├── railway.toml         # Railway config
├── requirements.txt     # Production dependencies
├── .env.example         # All config variables
├── README.md            # Full docs (KR+EN)
└── CLAUDE.md            # This file
```
