# Strategy Backlog

这个文件用于沉淀 **更广泛的量化研究候选**。
目标不是立刻把所有东西都实现，而是把候选池逐步扩出来，然后再筛成活跃 top 10。

## Current baseline

- `five-wave-reversal`
  - 状态：已有实现
  - 角色：baseline / 对照组
  - 说明：不要默认继续围绕它做小修小补，除非本轮是对照实验

## Candidate directions

### 1. Donchian Breakout
- 类型：趋势跟随
- 数据需求：OHLCV
- 状态：tested
- 假设：突破过去 N 根高点 / 低点后，存在延续性
- 最小实现：N 通道 + ATR 止损 + 时间或反向信号退出
- 最新实验：`20260413-103446__4h__bw20_ew10` 已产出最小 account prototype（4h，breakout_window=20，exit_window=10）
- 当前观察：年化 `20.9934%`、最大回撤 `94.1958%`、return/drawdown `0.2229`、`2455` 笔 closed trades；方向本身可形成稳定交易流，但裸 Donchian 回撤过深，下一步优先考虑 ATR stop 或 regime filter，而不是直接继续微调五浪

### 2. EMA Trend Following
- 类型：趋势跟随
- 数据需求：OHLCV
- 状态：tested
- 假设：快慢均线与价格位置可给出低复杂度趋势框架
- 最小实现：EMA cross + slope filter + trend break exit
- 最新实验：`20260413-123611__4h__fw20_sw50_sl5` 已产出最小 account prototype（4h，fast_window=20，slow_window=50，slope_window=5）
- 当前观察：年化 `43.5367%`、最大回撤 `94.3986%`、return/drawdown `0.4612`、`4774` 笔 closed trades；相对本地历史语料，它明显强于 Donchian 4h baseline（`0.2229`）与 five-wave 4h baseline（`0.2122`），但仍显著落后于 30m five-wave 主结果，且回撤极深。下一步优先考虑 ATR stop / regime filter / 更保守仓位框架，而不是直接做参数细抠

### 3. Z-score Mean Reversion
- 类型：均值回归
- 数据需求：OHLCV
- 状态：tested
- 假设：短期偏离统计均值后会有回归
- 最小实现：rolling mean/std + oversold z-score entry + mean-reversion / time-stop exit
- 最新实验：`20260413-143558__4h__lb20_ez2_xz0p5_h10` 已产出最小 account prototype（4h，lookback=20，entry_z=2.0，exit_z=0.5，max_hold_bars=10）
- 当前观察：年化 `-38.1879%`、最大回撤 `99.0896%`、return/drawdown `-0.3854`、`5316` 笔 closed trades；虽然名义胜率 `51.9752%`，但均值回归收益被手续费与尾部亏损明显吞掉，显著弱于当前本地趋势类 baseline（EMA `0.4612`、Donchian `0.2229`）与 five-wave 主结果。这个方向暂时更像“需要强过滤器”的反例，下一步若继续应优先考虑趋势/regime filter、波动率约束或更低频持仓，而不是直接刷参数。

### 4. Multi-timeframe Trend Alignment
- 类型：趋势共振
- 数据需求：OHLCV
- 假设：高周期方向 + 低周期入场，可改善胜率或回撤
- 最小实现：4h 定方向，30m/5m 触发入场

### 5. Volatility Compression Breakout
- 类型：波动率突破
- 数据需求：OHLCV
- 状态：tested
- 假设：波动率收缩后更容易出现方向性扩张
- 最小实现：BB width squeeze + Donchian-style breakout entry + channel/time-stop exit
- 最新实验：`20260413-164501__4h__sw20_bw55_ew20_sq0p2_h30` 已产出最小 account prototype（4h，squeeze_window=20，breakout_window=55，exit_window=20，squeeze_quantile=0.2，max_hold_bars=30）
- 当前观察：同一方向里，默认 `bw20/ew10/sq0.2/h20` 首轮结果很弱（return/drawdown `0.0450`），但把突破窗口拉长到 `55`、退出窗口放宽到 `20` 后，年化 `20.5218%`、最大回撤 `82.1394%`、return/drawdown `0.2498`、`1268` 笔 closed trades；它已经略好于 Donchian 4h baseline（`0.2229`），且回撤明显低于 EMA 4h baseline（`94.3986%`），说明“先压缩、后突破”的过滤方向有信息量。下一步优先考虑 ATR stop / regime filter / 持仓数量与仓位约束，而不是继续在五浪上打转。

### 6. Volume Spike Continuation / Reversal
- 类型：量价因子
- 数据需求：OHLCV
- 假设：异常成交量在不同市场状态下对应延续或反转
- 最小实现：volume z-score + candle direction + next-k bars return study

### 7. Cross-sectional Relative Strength
- 类型：横截面因子
- 数据需求：多币种 OHLCV
- 状态：tested
- 假设：相对强势币种在短中期更可能继续跑赢
- 最小实现：过去 N 日收益排序 + top bucket 持有回测
- 最新实验：`20260413-184155__1d__lb60_top5_h10_rb10` 已产出最小 account prototype（1d，lookback_bars=60，top_k=5，hold_bars=10，rebalance_interval=10）
- 当前观察：首轮参数扫描里，`1d / 60-bar lookback / top 5 / hold 10 bars` 是当前方向内最佳基线，年化 `33.8572%`、最大回撤 `67.6178%`、return/drawdown `0.5007`、`892` 笔 closed trades、`100` 个币种参与；它明显强于 Donchian 4h baseline（`0.2229`）、Volatility Compression 4h baseline（`0.2498`）和 EMA 4h baseline（`0.4612`），但仍显著落后于 30m five-wave 两个头部结果。这个方向说明“多币种横截面动量轮动”本身就有信息量，下一步优先考虑 absolute momentum / regime filter / 权重与换手约束，而不是回去继续微调五浪。

### 8. Short-term Reversal Basket
- 类型：横截面反转
- 数据需求：多币种 OHLCV
- 假设：极端弱势币种在短窗口存在反弹修复
- 最小实现：过去 1-3 日跌幅分组 + next window 收益测试

### 9. Regime Filter
- 类型：市场状态过滤
- 数据需求：OHLCV
- 假设：同一策略在趋势市和震荡市表现差异明显
- 最小实现：ADX / ATR / rolling trend strength 过滤器

### 10. ATR-based Risk Framework
- 类型：风控框架
- 数据需求：OHLCV
- 假设：统一的 ATR 仓位和止损框架可以提升跨策略可比性
- 最小实现：position sizing + stop distance + trailing logic

### 11. Momentum + Volatility Composite
- 类型：复合因子
- 数据需求：OHLCV
- 状态：tested
- 假设：动量与波动率联合排序比单一因子更稳
- 最小实现：过去 N 收益 ÷ 实现波动率 的横截面排序 + 固定周期轮动
- 最新实验：`20260413-204337__1d__lb60_vw60_top5_h5_rb5_mv0p5` 已产出最小 account prototype（1d，lookback_bars=60，volatility_window=60，top_k=5，hold_bars=5，rebalance_interval=5，min_volatility_pct=0.5）
- 当前观察：这个方向对参数比较敏感，默认 `lb60/vw20/top5/h10/rb10` 很弱（return/drawdown `0.1175`），但把波动率窗口拉长到 `60` 且把轮动节奏加快到 `5 bars` 后，年化 `45.0105%`、最大回撤 `59.7927%`、return/drawdown `0.7528`、`1690` 笔 closed trades、`100` 个币种参与。它显著强于纯 Cross-sectional Relative Strength 当前 best（`0.5007`）、EMA 4h（`0.4612`）、Volatility Compression 4h（`0.2498`）和 Donchian 4h（`0.2229`），但仍落后于 30m five-wave 两个头部结果（`3.6473` / `1.0916`）。说明“动量 + 低波动惩罚”的横截面复合排序是当前候选池里更值得继续扩的方向。下一步优先考虑 absolute momentum gate、换手/手续费约束和权重方案，而不是回去默认继续微调五浪。

### 12. Pullback Entry in Trend
- 类型：趋势回撤入场
- 数据需求：OHLCV
- 假设：趋势中的回撤买点比追突破有更好盈亏比
- 最小实现：趋势过滤 + 回撤到均线/通道中位后再入场

## Working rules

- 每个 2 小时实验周期优先从这里挑一个方向推进
- 若没有实现条件，先补最小研究脚手架或先写清实验设计
- 每次推进后更新状态：`idea` / `prototype` / `tested` / `promoted`
- 只有进入活跃前 10 的结果，才更新 `research/leaderboard.json`
