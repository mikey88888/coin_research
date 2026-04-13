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

### 3. Multi-timeframe Trend Alignment
- 类型：趋势共振
- 数据需求：OHLCV
- 假设：高周期方向 + 低周期入场，可改善胜率或回撤
- 最小实现：4h 定方向，30m/5m 触发入场

### 4. Z-score Mean Reversion
- 类型：均值回归
- 数据需求：OHLCV
- 假设：短期偏离统计均值后会有回归
- 最小实现：rolling mean/std + z-score threshold + time stop

### 5. Volatility Compression Breakout
- 类型：波动率突破
- 数据需求：OHLCV
- 假设：波动率收缩后更容易出现方向性扩张
- 最小实现：rolling ATR percentile / BB width squeeze + breakout entry

### 6. Volume Spike Continuation / Reversal
- 类型：量价因子
- 数据需求：OHLCV
- 假设：异常成交量在不同市场状态下对应延续或反转
- 最小实现：volume z-score + candle direction + next-k bars return study

### 7. Cross-sectional Relative Strength
- 类型：横截面因子
- 数据需求：多币种 OHLCV
- 假设：相对强势币种在短中期更可能继续跑赢
- 最小实现：过去 N 日收益排序 + top bucket 持有回测

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
- 假设：动量与波动率联合排序比单一因子更稳
- 最小实现：过去 N 收益 * volatility penalty 的横截面排序

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
