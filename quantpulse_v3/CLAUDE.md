# QuantPulse v3 — Project Memory (Final)

## Architecture
- 7-agent multi-agent quant trading system (MIA, QR, CSO, CRCO, ES, PO, IMA)
- Cloud-native (Railway/Replit/Docker)
- Async MessageBus (PubSub + request-response + priority queue)
- SQLite AuditLogger for every decision
- Binance Futures connector (CCXT) + PaperExchange
- PriceFeed service (exchange → MIA + ES)
- 4-layer safety (gate + CRCO + circuit breaker + emergency stop)

## Implementation Status — FULLY OPERATIONAL
| Phase | Contents | Tests |
|-------|----------|-------|
| 1-4 | Core + agents + orchestrator | 86 |
| 5-8 | Exchanges, validation, safety, live activation | 39 |
| P0/P1 | OHLCV, stop orders, recovery, retry, config, partial TP | - |
| **Total** | **Complete** | **125** |

## P0/P1 Improvements Completed

### P0#1-3: MIA Real OHLCV Integration
- `_gather_market_data()` fetches real OHLCV from exchange via `fetch_ohlcv()` with 10s timeout
- `_compute_smc_from_ohlcv()`: detects BOS, CHOCH, FVG, order blocks, liquidity from real candles
- `_compute_trend()`: EMA(8)/EMA(21) crossover for direction + strength from candle closes
- `_extract_key_levels()`: swing high/low pivot detection for support/resistance
- `_compute_atr_pct()`: 14-period ATR from OHLCV, clamped 0.5%-10%, replaces hardcoded 2%

### P0#4: Exchange Stop Orders
- `_place_stop_order()`: creates STOP_MARKET order on exchange as SL safety net
- Binance connector: supports market, limit, stop_market, stop_limit order types
- PaperExchange: stores stop orders without immediate execution

### P0#5: Position Recovery
- `_recover_positions()`: on startup, fetches open positions from exchange
- Rebuilds `_open_trades` with recovered data, assigns default SL/TP
- Logs each recovered position to audit trail

### P0#6: Retry Logic
- `_execute_live()` uses `retry_async()` (3 retries, exponential backoff)
- All exchange calls wrapped in `asyncio.wait_for(timeout=30s)`
- Leverage setting has separate 10s timeout (best-effort, non-blocking)

### P0#7: Telegram Queue Fix
- Queue capped at 100 messages (FIFO eviction when exceeded)
- Prevents unbounded memory growth in long-running sessions

### P0#8: Timeouts
- PriceFeed: `fetch_ticker()` wrapped in 10s timeout
- MIA: `fetch_ohlcv()` wrapped in 10s timeout per timeframe
- ES: all exchange calls have explicit timeouts (10-30s)

### P1#9-10: Config Extraction (CRCO + ES)
All previously hardcoded values now in Config with env var support:
- ES: slippage_bps, commission_rate, trailing_stop_pct
- CRCO: max_drawdown, max_single_exposure, max_consecutive_losses, max_daily_trades, max_portfolio_heat, initial_capital
- MIA: analysis_interval, default_volatility_pct
- TP: tp1_close_pct (50%), tp2_close_pct (30%), tp3_close_pct (20%)

### P1#11: Partial Take-Profit
- TP1 hit: close 50% of position, keep 50% running
- TP2 hit: close 30% of remaining
- TP3 hit: close everything remaining
- Each partial close publishes `es.order_closed` with `partial: true`

## Remaining Priorities (Post P0/P1)

### P2 — MEDIUM
| # | Item | Status |
|---|------|--------|
| 1 | Clean stale Phase comments in code | Easy |
| 2 | QR scoring: use real data instead of defaults | Hard |
| 3 | PriceFeed reconnect backoff | Easy |
| 4 | PO initial_capital from config | Easy |

### P3 — LOW
| # | Item |
|---|------|
| 5 | Signal deduplication |
| 6 | Bybit connector |
| 7 | WebSocket price feed |
| 8 | Web dashboard (FastAPI) |
| 9 | Backtesting engine |

## Critical Patterns
- **NEVER** `request()` inside `subscribe_safe()` handler → `create_task()`
- **ALWAYS** wrap exchange calls in `asyncio.wait_for()` with timeout
- **ALWAYS** use `retry_async()` for live order execution
- **Boot order**: IMA→PO→ES→CRCO→QR→CSO→MIA
