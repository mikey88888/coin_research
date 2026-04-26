# Coin Research

独立于 `a_share_research` 的币市量化研究项目，当前聚焦于：

- 统一接入交易所行情（`ccxt`）
- 将现货市场与 OHLCV 数据沉淀到 PostgreSQL
- 对加密资产做研究型回测
- 用 FastAPI/Jinja 提供轻量研究看板
- 为后续策略扩展、实盘执行和自动化研究留接口

## 当前能力概览

目前仓库已经具备的核心能力：

- **交易所接入**：通过 `ccxt` 接 Binance / OKX / Bybit 等交易所
- **市场列表同步**：拉取可交易市场并写入 `market_data.crypto_markets`
- **K 线抓取与落库**：抓取 OHLCV 并 upsert 到 `market_data.crypto_ohlcv`
- **Top 币种批量同步**：按市值筛选可映射到交易所 `USDT` 现货的主流币，并分周期补齐历史数据
- **策略研究**：内置五浪加速下跌反转策略与账户制回测引擎
- **研究看板**：提供市场总览、币列表、币详情、回测记录、策略对比页面
- **测试基础**：已覆盖配置、数据抓取规范化、数据库辅助函数、同步核心逻辑

## 目录结构

```text
coin_research/
├─ src/coin_research/
│  ├─ cli.py                     # 命令行入口
│  ├─ config.py                  # 环境变量与运行配置
│  ├─ exchanges.py               # CCXT 交易所工厂
│  ├─ data.py                    # 市场/ohlcv 拉取与标准化
│  ├─ db.py                      # PostgreSQL schema + upsert/load
│  ├─ sync.py                    # Top 币种历史数据同步
│  ├─ backtest_five_wave_reversal.py
│  ├─ backtests/account.py       # 账户制回测引擎
│  ├─ strategies/five_wave_reversal.py
│  ├─ services/                  # 研究页/市场页服务层
│  ├─ web/app.py                 # FastAPI 应用入口
│  ├─ web/routes/pages.py        # 页面路由
│  ├─ templates/                 # Jinja 模板
│  └─ static/                    # CSS / JS
├─ scripts/
│  ├─ start_local_postgres.sh
│  └─ stop_local_postgres.sh
├─ tests/
├─ reports/
├─ notebooks/
├─ .env.example
├─ pyproject.toml
└─ uv.lock
```

## 环境要求

- Python `>=3.12`
- `uv`
- PostgreSQL 16（本地或远程均可）

建议在项目根目录操作：

```bash
cd /home/thinkpad/quant/coin_research
```

## 安装与初始化

### 1. 安装依赖

```bash
uv sync --dev
```

### 2. 配置环境变量

```bash
cp .env.example .env
```

关键变量：

- `COIN_RESEARCH_EXCHANGE`：默认交易所，默认 `binance`
- `COIN_RESEARCH_API_KEY`
- `COIN_RESEARCH_API_SECRET`
- `COIN_RESEARCH_ENABLE_RATE_LIMIT`
- `COIN_RESEARCH_TIMEOUT_MS`
- `COIN_RESEARCH_PG_DSN`：PostgreSQL 连接串

### 3. 启动本地 PostgreSQL（可选）

```bash
./scripts/start_local_postgres.sh
export COIN_RESEARCH_PG_DSN="postgresql://thinkpad@127.0.0.1:5432/coin_research"
```

### 4. 初始化数据库 schema

项目现在会自动读取仓库根目录的 `.env`，所以只要 `.env` 已存在，直接执行即可：

```bash
uv run coin-research db-init
```

## 命令行使用

### 列出交易所市场

```bash
uv run coin-research markets --exchange binance
```

按报价货币过滤：

```bash
uv run coin-research markets --exchange binance --quote USDT
```

写入数据库：

```bash
uv run coin-research markets --exchange binance --quote USDT --to-db
```

### 拉取 OHLCV

抓取并打印前几行：

```bash
uv run coin-research ohlcv --exchange binance --symbol BTC/USDT --timeframe 1h --limit 200
```

导出到 CSV：

```bash
uv run coin-research ohlcv --exchange binance --symbol ETH/USDT --timeframe 15m --limit 500 --output reports/eth_usdt_15m.csv
```

抓取并写入 PostgreSQL：

```bash
uv run coin-research ohlcv --exchange binance --symbol BTC/USDT --timeframe 4h --limit 500 --to-db
```

### 批量同步市值前 N 币

同步市值前 100 的可映射币种：

```bash
uv run coin-research sync-top --exchange binance
```

小范围试跑：

```bash
uv run coin-research sync-top --exchange binance --symbols-limit 5
```

如果当前 WSL 环境无法访问交易所公网接口、但你只想先把本地研究看板恢复到可浏览状态，可以先写入一批最小演示行情样本：

```bash
uv run python scripts/seed_demo_market_data.py
```

当前同步策略默认按以下时间窗补数据：

- `1d`：全历史
- `4h`：全历史
- `30m`：最近三年
- `5m`：最近半年

## 数据库设计

- 环境变量：`COIN_RESEARCH_PG_DSN`
- schema：`market_data`
- 表：
  - `market_data.crypto_markets`
  - `market_data.crypto_ohlcv`
  - 以及供网页加速使用的预聚合统计表

主键设计：

- `crypto_markets`: `(exchange, symbol)`
- `crypto_ohlcv`: `(exchange, symbol, timeframe, bar_time)`

默认约定：

- 时间统一存 `UTC`；网页展示和模拟盘人工日志统一显示为北京时间
- 使用 `ON CONFLICT DO UPDATE` 做幂等 upsert
- `ccxt` 读取 shell 的代理环境变量，适配 WSL 代理场景

## 回测

当前内置策略：**五浪加速下跌反转**。

示例：

```bash
export COIN_RESEARCH_PG_DSN="postgresql://thinkpad@127.0.0.1:5432/coin_research"
uv run python -m coin_research.backtest_five_wave_reversal \
  --exchange binance \
  --timeframe 4h \
  --exit-mode three_wave_exit \
  --engine account
```

回测产物默认写到：

```text
reports/backtests/five-wave-reversal/<run_id>/
```

其中通常包含：

- `summary.json`
- `run_meta.json`
- `trades.csv`
- `orders.csv`（账户制回测）
- `equity_curve.csv`（账户制回测）

## Web 研究看板

启动方式：

项目会自动读取仓库根目录的 `.env`，所以本地默认情况下直接跑即可：

```bash
uv run python -m coin_research.web.app --host 0.0.0.0 --port 8001
```

如果你确认依赖已经同步好，也可以：

```bash
uv run --no-sync python -m coin_research.web.app --host 0.0.0.0 --port 8001
```

主要页面：

- `/`
- `/markets`
- `/markets/crypto`
- `/markets/crypto/{symbol}`
- `/paper`
- `/research/runs`
- `/research/runs/{run_id}`
- `/research/strategies/five-wave-reversal`

## 真实行情模拟盘

当前已提供一个 **Binance Spot USDT** 的网页控制模拟盘入口：

- 页面：`/paper`
- 运行形态：网页发起，后台子进程 runner 执行
- 策略：`Absolute Momentum Gated Composite`
- 默认周期：`30m`

它使用 **真实公网 K 线** 驱动策略与模拟成交，但 **不会调用币安私有下单 API**。

使用前请先确保 PostgreSQL 可用并已初始化 schema：

```bash
./scripts/start_local_postgres.sh
export COIN_RESEARCH_PG_DSN="postgresql://thinkpad@127.0.0.1:5432/coin_research"
uv run coin-research db-init
```

如果 `/paper/start` 提示 Binance 连接失败，先运行诊断命令。它会分别检查 WSL 直连、当前 `HTTP(S)_PROXY`、WSL gateway 代理和 ccxt `exchangeInfo`：

```bash
uv run --no-sync coin-research diagnose-binance
```

然后启动 Web：

```bash
uv run --no-sync python -m coin_research.web.app --host 0.0.0.0 --port 8001
```

打开 `/paper` 后即可：

- 启动一条模拟盘 session
- 查看当前持仓
- 查看最近订单
- 查看净值曲线
- 请求停止当前 session

## 测试与基础验证

先同步 dev 依赖：

```bash
uv sync --dev
```

运行测试：

```bash
uv run pytest -q
```

当前测试主要覆盖：

- 配置加载
- OHLCV 标准化
- PostgreSQL 辅助函数
- Top 币种同步的关键逻辑

## 推荐工作流

### 首次建库

```bash
uv sync --dev
cp .env.example .env
./scripts/start_local_postgres.sh
export COIN_RESEARCH_PG_DSN="postgresql://thinkpad@127.0.0.1:5432/coin_research"
uv run coin-research db-init
uv run coin-research markets --exchange binance --quote USDT --to-db
uv run coin-research sync-top --exchange binance --symbols-limit 5
```

### 正常研究流程

1. 先同步目标交易所的市场与历史 K 线
2. 跑单策略回测
3. 打开网页看总览、单币详情和回测结果
4. 再决定是否扩策略、扩因子、扩风控

## 自动化建议

这个仓库的下一阶段目标，不是盲目“自动写代码”，而是先把自动化护栏补齐：

- 测试依赖完整
- 测试/烟测命令固定
- Codex 工作目录收敛在独立仓库内
- 每轮自动优化只做一个小改动
- 改动后必须过测试或烟测
- 默认不自动 push，先由人工审核

在此基础上，再接“每 2 小时生成 1 个小优化点并交给 Codex 执行”的循环会更稳。

## 已知限制

- 目前交易所接入、同步策略和研究页仍偏研究型，不是实盘交易系统
- 回测策略还比较少，暂时以内置五浪策略为主
- 测试覆盖还不算完整，尤其是网页层和回测联调层
- 自动化工作流尚未正式接入定时调度

## 后续方向

- 增加更多研究型策略与因子实验
- 补齐网页层和回测层的测试
- 增加更明确的 smoke 命令
- 增加自动化研究/审查/回归流程
- 在稳定后再接实盘执行与风控模块

## 停止本地 PostgreSQL

```bash
./scripts/stop_local_postgres.sh
```
