# QuantPulse v3 — Project Memory

## Architecture
- 7-agent multi-agent quant trading system
- Async MessageBus + SQLite AuditLogger + 4-layer safety
- Binance Futures (CCXT) with testnet support + PaperExchange
- Real OHLCV analysis: EMA trend, BOS/CHOCH/FVG/OB, ATR volatility
- Stop orders on exchange, position recovery, retry logic, partial TP

## Stats
- Files: 42 | LOC: ~9,700 | Tests: 130 (all passing) | Commits: 14

## Implementation Complete
| Category | Contents |
|----------|----------|
| Core | models, message_bus, audit_logger, base_agent, config |
| Agents | MIA, QR, CSO, CRCO, ES, PO, IMA (all 7) |
| Exchange | BinanceFutures (CCXT, testnet support) + PaperExchange |
| Safety | 3-stage gate, CRCO 11 checks, circuit breaker, emergency stop |
| Analysis | Real OHLCV → EMA trend, swing pivots, BOS/CHOCH/FVG/OB, ATR |
| Execution | Stop orders, position recovery, retry(3x), partial TP(50/30/20) |
| Deploy | Docker, Railway, start.sh, healthcheck.sh |

## Testnet Support
- `EXCHANGE_TESTNET=true` in .env → auto-enables sandbox mode
- Binance testnet: https://testnet.binancefuture.com
- JWQuantSystem boot log shows "TESTNET" or "MAINNET"
- All safety mechanisms (gate, CRCO, CB, estop) work in testnet

## How to Start Testnet
```bash
# 1. Get testnet API keys from https://testnet.binancefuture.com
# 2. Edit .env:
BINANCE_API_KEY=your_testnet_key
BINANCE_API_SECRET=your_testnet_secret
EXCHANGE_TESTNET=true
TRADE_MODE=PAPER   # Start in PAPER, switch to LIVE after observation
# 3. Install ccxt: pip install ccxt
# 4. Run: python main.py
# 5. After observation: await system.switch_mode("LIVE")
```

## All Config Parameters (env vars)
| Var | Default | Description |
|-----|---------|-------------|
| TRADE_MODE | PAPER | PAPER or LIVE |
| EXCHANGE_TESTNET | false | Use testnet/sandbox |
| MAX_RISK_PER_TRADE | 0.01 | 1% per trade |
| MAX_DAILY_LOSS | 0.03 | 3% daily limit |
| CIRCUIT_BREAKER_LOSS | 0.05 | 5% triggers halt |
| SLIPPAGE_BPS | 2.0 | Paper slippage |
| COMMISSION_RATE | 0.0004 | 0.04% fee |
| TRAILING_STOP_PCT | 0.005 | 0.5% trail |
| TP1_CLOSE_PCT | 0.5 | 50% at TP1 |
| TP2_CLOSE_PCT | 0.3 | 30% at TP2 |
| TP3_CLOSE_PCT | 0.2 | 20% at TP3 |
| INITIAL_CAPITAL | 10000 | Starting USD |
| MAX_DRAWDOWN | 0.10 | 10% max DD |
| MAX_CONSECUTIVE_LOSSES | 5 | Loss streak limit |
| MAX_DAILY_TRADES | 20 | Daily trade cap |

## Critical Patterns
- NEVER `request()` inside `subscribe_safe()` handler → `create_task()`
- ALWAYS `asyncio.wait_for()` on exchange calls (10-30s timeout)
- ALWAYS `retry_async()` for live order execution
- Boot order: IMA→PO→ES→CRCO→QR→CSO→MIA

## Remaining (P2-P3)
- QR real data scoring, PriceFeed backoff, Bybit connector
- WebSocket feed, web dashboard, backtesting engine
