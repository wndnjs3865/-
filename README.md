# QuantPulse v3 — Multi-Agent Quant Cloud Trading System

**7개 전문 AI 에이전트가 협력하는 클라우드 기반 자동매매 시스템**

> A cloud-native automated trading system powered by 7 specialized AI agents with SMC/ICT analysis, absolute risk governance, and full audit trail.

---

## Architecture / 아키텍처

```
MIA (Market Intelligence) → CSO (Strategy Officer) → ES (Execution)
         ↕                        ↕
QR (Quant Researcher)       CRCO (Risk Officer) ← absolute veto
         ↕                        ↕
PO (Performance)            IMA (Monitoring + Alerts)
```

| Agent | Role | 역할 |
|-------|------|------|
| **MIA** | Market Intelligence Analyst | SMC/ICT + 멀티소스 시장 분석 |
| **QR** | Quant Researcher | 8차원 정량 스코어링 |
| **CSO** | Chief Strategy Officer | 파이프라인 오케스트레이터 |
| **CRCO** | Chief Risk & Compliance Officer | 11개 리스크 체크 + 절대 veto권 |
| **ES** | Execution Specialist | Paper/Live 주문 실행 + SL/TP/Trailing |
| **PO** | Performance Optimizer | 성과 저널 + Sharpe/MDD/PF 분석 |
| **IMA** | Infrastructure & Monitoring | 시스템 헬스 + Telegram 알림 |

## Key Features / 핵심 기능

- **CRCO Absolute Veto**: 11개 리스크 체크 중 하나라도 실패하면 거래 불가. CSO도 무시 불가.
- **Circuit Breaker**: 일일 손실 한도 초과 시 전 에이전트에 즉시 전파, 거래 중단.
- **Full Audit Trail**: 모든 결정(신호, 스코어, 리스크, 주문)이 SQLite에 기록.
- **Async MessageBus**: 우선순위 큐 + correlation_id 기반 요청-응답 패턴.
- **SMC/ICT Analysis**: Break of Structure, CHOCH, Order Block, FVG, Liquidity.
- **Paper Trading**: 시뮬레이션 슬리피지(2bps) + 수수료(4bps) + 트레일링 스탑.

## Quick Start / 빠른 시작

### 1. Clone & Setup
```bash
git clone https://github.com/wndnjs3865/-.git
cd quantpulse_v3
cp .env.example .env
# Edit .env with your settings
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run
```bash
python main.py
```

### Docker
```bash
docker compose up -d
```

### Railway / Replit
- Push to GitHub → Connect to Railway/Replit
- Set environment variables from `.env.example`
- Start command: `python main.py`

## Configuration / 설정

All settings via environment variables (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `TRADE_MODE` | `PAPER` | PAPER or LIVE |
| `MAX_RISK_PER_TRADE` | `0.01` | 1% per trade |
| `MAX_DAILY_LOSS` | `0.03` | 3% daily loss limit |
| `MAX_LEVERAGE` | `10.0` | Maximum leverage |
| `MIN_QUANT_SCORE` | `60.0` | Minimum QR score |
| `CIRCUIT_BREAKER_LOSS` | `0.05` | 5% triggers halt |

## Message Flow / 메시지 흐름

```
MIA → mia.trade_signal → CSO
  CSO → cso.request_quant_score → QR → qr.quant_score → CSO
  CSO → cso.request_risk_assessment → CRCO → crco.risk_assessment → CSO
  CSO → cso.trade_decision → ES
    ES → es.order_filled → PO, CRCO
    ES → es.order_closed → PO, CRCO
CRCO → crco.circuit_breaker → ALL agents (broadcast)
IMA ← ima.alert (from any agent)
```

## Testing / 테스트

```bash
pip install pytest pytest-asyncio
python -m pytest tests/ -v
```

80+ tests covering: models, MessageBus, AuditLogger, all 7 agents, full E2E pipeline, circuit breaker cascade.

## Project Structure / 프로젝트 구조

```
quantpulse_v3/
├── main.py              # JWQuantSystem orchestrator
├── config.py            # Environment-based configuration
├── core/                # Infrastructure
│   ├── models.py        # 17 Pydantic models
│   ├── message_bus.py   # Async PubSub + priority queue
│   ├── audit_logger.py  # SQLite audit log
│   └── base_agent.py    # Agent abstract base class
├── agents/              # 7 specialized agents
├── exchanges/           # Exchange connectors
│   ├── base_exchange.py # Abstract exchange interface
│   └── paper_exchange.py# Paper trading simulator
├── tests/               # 80+ tests
├── scripts/
│   ├── start.sh         # Cloud deployment entrypoint
│   └── healthcheck.sh   # Health check script
├── Dockerfile
├── docker-compose.yml
├── railway.toml
└── requirements.txt
```

---

## Risk Warning / 위험 경고

> **WARNING**: This software is for educational and research purposes. Cryptocurrency and stock trading involves substantial risk of loss. Past performance does not guarantee future results. Never trade with money you cannot afford to lose. The developers are not responsible for any financial losses.

> **경고**: 이 소프트웨어는 교육 및 연구 목적입니다. 암호화폐 및 주식 거래는 상당한 손실 위험을 수반합니다. 과거 성과가 미래 수익을 보장하지 않습니다. 잃어도 되는 금액 이상으로 거래하지 마십시오. 개발자는 금전적 손실에 대해 책임지지 않습니다.

---

## License

MIT
