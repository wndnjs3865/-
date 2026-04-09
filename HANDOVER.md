# QuantPulse v3 — Termux Claude Code 인수인계 문서

## 이 문서의 목적
이전 Claude Code 세션("quantpulse v3")에서 완성한 전체 프로젝트를
Termux의 Claude Code가 이어받아 작업하기 위한 완전한 컨텍스트 문서.

## 프로젝트 개요
- 7-agent 멀티에이전트 자동매매 시스템 (Crypto Futures)
- Python 3.11+, 비동기(asyncio) 기반
- 130개 테스트 전부 통과
- 42개 파일, ~9,700줄

## 현재 코드 위치
```bash
git clone -b fresh-main https://github.com/wndnjs3865/- ~/quantpulse_v3
cd ~/quantpulse_v3
```

## 프로젝트 구조
```
~/quantpulse_v3/
├── main.py              # JWQuantSystem 오케스트레이터
├── config.py            # 모든 설정 (환경변수 기반)
├── core/                # 핵심 인프라
│   ├── models.py        # 17개 Pydantic 모델
│   ├── message_bus.py   # 비동기 PubSub + 우선순위 큐
│   ├── audit_logger.py  # SQLite 감사 로그
│   ├── base_agent.py    # 에이전트 ABC
│   ├── enums.py         # 열거형 (Priority, Direction 등)
│   └── price_feed.py    # 실시간 가격 피드
├── agents/              # 7개 에이전트
│   ├── mia.py           # Market Intelligence (OHLCV+SMC/ICT)
│   ├── qr.py            # Quant Researcher (8차원 스코어링)
│   ├── cso.py           # Chief Strategy Officer (오케스트레이터)
│   ├── crco.py          # Risk Officer (11체크+veto+circuit breaker)
│   ├── es.py            # Execution Specialist (주문+SL/TP+부분익절)
│   ├── po.py            # Performance Optimizer (저널+Sharpe)
│   └── ima.py           # Infrastructure Monitor (헬스+텔레그램)
├── exchanges/           # 거래소 커넥터
│   ├── base_exchange.py # 추상 인터페이스
│   ├── binance_futures.py # Binance CCXT 커넥터
│   └── paper_exchange.py  # 시뮬레이션 거래소
├── tests/               # 130개 테스트 (15개 파일)
├── .env.example         # 환경변수 템플릿
├── Dockerfile           # 멀티스테이지 빌드
├── docker-compose.yml
├── railway.toml
├── requirements.txt     # pydantic, psutil, aiofiles, python-dotenv
├── CLAUDE.md            # 프로젝트 메모리
├── README.md
└── LIVE_GUIDE.md        # 라이브 트레이딩 가이드
```

## 7개 에이전트 메시지 흐름
```
MIA → mia.trade_signal → CSO (create_task로 파이프라인 실행)
  CSO → cso.request_quant_score → QR → qr.quant_score → CSO
  CSO → cso.request_risk_assessment → CRCO → crco.risk_assessment → CSO
  CSO → cso.trade_decision → ES
    ES → es.order_submitted/filled → PO, CRCO
    ES → es.order_closed → PO, CRCO
CRCO → crco.circuit_breaker → CSO, ES, MIA (broadcast)
IMA ← ima.alert (from any agent)
```

## 완성된 기능 (Phase 1~Final)
1. 핵심 인프라: MessageBus, AuditLogger, BaseAgent, Config
2. 7개 에이전트 전부 구현 + 상호작용 검증
3. 4중 안전장치: Safety Gate, CRCO 11체크, Circuit Breaker, Emergency Stop
4. MIA: 실제 OHLCV → EMA 트렌드, BOS/CHOCH/FVG/OB, ATR 변동성
5. ES: Stop Order, 포지션 복구, 재시도(3회), 부분 익절(50/30/20)
6. Binance Futures 커넥터 (CCXT, 테스트넷 지원)
7. PriceFeed 서비스 (거래소 → MIA + ES)
8. Telegram 알림 (urllib, 큐 100개 제한)
9. 모든 파라미터 Config화 (.env 환경변수)
10. Docker/Railway/Replit 배포 준비

## 핵심 패턴 (반드시 지킬 것)
- **NEVER** `request()` inside `subscribe_safe()` handler → `create_task()`
- **ALWAYS** `asyncio.wait_for()` on exchange calls (10-30s timeout)
- **ALWAYS** `retry_async()` for live order execution
- **Boot order**: IMA→PO→ES→CRCO→QR→CSO→MIA (소비자 먼저)

## 발견하고 수정한 버그 6개
1. MessageBus: request 메시지에서 future 해소 → topic 매칭으로 수정
2. ES: 수수료 quantity 기반 → notional (price*qty*rate) 기반
3. Trailing stop: 활성화 시 trail_price 미리 설정 → 제거
4. CSO: handler 안에서 request() → create_task()로 데드락 방지
5. ES: _check_sl_tp 미구현 → 가격 피드 연동 완성
6. PaperExchange: stop_market 주문 즉시 실행 → 저장만 하도록 수정

## 남은 우선순위
### P2 (권장)
- QR 실데이터 스코어링 (현재 기본값 60점)
- PriceFeed 재연결 백오프
- PO initial_capital Config 연동
- 신호 중복 방지

### P3 (확장)
- Bybit 커넥터
- WebSocket 가격 피드
- 웹 대시보드 (FastAPI)
- 백테스팅 엔진

## Termux 설치 명령어
```bash
pkg update -y && pkg install -y python git
rm -rf ~/quantpulse ~/quantpulse_v2 ~/jwquant 2>/dev/null
git clone -b fresh-main https://github.com/wndnjs3865/- ~/quantpulse_v3
cd ~/quantpulse_v3
pip install pydantic psutil aiofiles
python -m pytest tests/ -v  # 130개 통과 확인
python main.py               # 시스템 실행
```

## 테스트넷 설정
```bash
export BINANCE_API_KEY=PlmmbIT3Rbku8oOGIcg7HOLs4AERGAdV919ZuHOh3kvMVK5OP84yCFWwzT37CsMw
export BINANCE_API_SECRET=KYZhsrwfdw3QjmlvCT6n1HrLnt4ihbsR2aZ2NDxXmdrIuF5G9a3ozwg5bL4ihdg0
export EXCHANGE_TESTNET=true
export TRADE_MODE=PAPER
pip install ccxt
python main.py
# → "Network: TESTNET" + "ALL 7 AGENTS OPERATIONAL" 확인
```

## 이 문서를 Termux Claude Code에 전달하는 방법
Termux에서 Claude Code 실행 후:
```
이 파일을 읽어줘: ~/quantpulse_v3/HANDOVER.md
이 프로젝트의 CLAUDE.md도 읽어줘: ~/quantpulse_v3/CLAUDE.md
이전 세션에서 완성한 프로젝트를 이어서 작업해야 해.
```
