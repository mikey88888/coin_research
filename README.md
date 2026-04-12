# Coin Research

独立于 `a_share_research` 的币市量化研究项目。

第一版目标：
- 用 `CCXT` 统一接交易所
- 拉取现货/合约 K 线做研究
- 为后续策略回测和实盘下单预留结构

## 目录

- `src/coin_research/config.py`
  - 环境变量和运行配置
- `src/coin_research/exchanges.py`
  - `CCXT` 交易所工厂
- `src/coin_research/data.py`
  - K 线拉取和 DataFrame 规范化
- `src/coin_research/cli.py`
  - 命令行入口
- `src/coin_research/strategies/`
  - 后续放策略
- `src/coin_research/backtests/`
  - 后续放回测引擎
- `src/coin_research/live/`
  - 后续放实盘执行

## 快速开始

```bash
cd /home/thinkpad/quant/coin_research
uv sync
cp .env.example .env
./scripts/start_local_postgres.sh
export COIN_RESEARCH_PG_DSN="postgresql://thinkpad@127.0.0.1:5432/coin_research"
uv run coin-research db-init
uv run coin-research markets --exchange binance
uv run coin-research ohlcv --exchange binance --symbol BTC/USDT --timeframe 1h --limit 200
```

## 当前命令

列出交易所可交易市场：

```bash
uv run coin-research markets --exchange binance
```

写入 PostgreSQL 市场表：

```bash
uv run coin-research markets --exchange binance --quote USDT --to-db
```

抓取 K 线并输出前几行：

```bash
uv run coin-research ohlcv --exchange binance --symbol BTC/USDT --timeframe 1h --limit 200
```

导出到 CSV：

```bash
uv run coin-research ohlcv --exchange binance --symbol ETH/USDT --timeframe 15m --limit 500 --output reports/eth_usdt_15m.csv
```

抓取 K 线并写入 PostgreSQL：

```bash
uv run coin-research ohlcv --exchange binance --symbol BTC/USDT --timeframe 4h --limit 500 --to-db
```

初始化数据库 schema：

```bash
uv run coin-research db-init
```

同步市值前 100 币到本地库：

```bash
uv run coin-research sync-top --exchange binance
```

先做小范围试跑：

```bash
uv run coin-research sync-top --exchange binance --symbols-limit 5
```

运行五浪加速下跌反转回测：

```bash
export COIN_RESEARCH_PG_DSN="postgresql://thinkpad@127.0.0.1:5432/coin_research"
uv run python -m coin_research.backtest_five_wave_reversal --exchange binance --timeframe 4h --exit-mode three_wave_exit --engine account
```

启动网页：

```bash
export COIN_RESEARCH_PG_DSN="postgresql://thinkpad@127.0.0.1:5432/coin_research"
uv run python -m coin_research.web.app --host 0.0.0.0 --port 8001
```

停止本地 PostgreSQL：

```bash
./scripts/stop_local_postgres.sh
```

## 数据库设计

- 使用本地 PostgreSQL 16，风格对齐 `a_share_research`
- 环境变量：`COIN_RESEARCH_PG_DSN`
- schema：`market_data`
- 表：
  - `market_data.crypto_markets`
  - `market_data.crypto_ohlcv`

主键设计：

- `crypto_markets`: `(exchange, symbol)`
- `crypto_ohlcv`: `(exchange, symbol, timeframe, bar_time)`

默认约定：

- 时间统一存 `UTC`
- 通过 `ON CONFLICT DO UPDATE` 做幂等 upsert
- `ccxt` 默认会继承 shell 里的代理环境变量，适配 WSL 代理场景
- `sync-top` 的默认时间窗：
  - `1d`: 全历史
  - `4h`: 全历史
  - `30m`: 最近三年
  - `5m`: 最近半年
- 网页入口：
  - `/`
  - `/markets/crypto`
  - `/markets/crypto/{symbol}`
  - `/research/runs`
  - `/research/strategies/five-wave-reversal`

## 下一步建议

- 先做一个事件型策略研究模块
- 再加一个账户制回测引擎
- 最后接实盘执行和风控
