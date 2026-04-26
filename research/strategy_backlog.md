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
- 状态：tested
- 假设：极端弱势币种在短窗口存在反弹修复
- 最小实现：过去 N bars 收益倒序排序 + 买入 bottom bucket + 固定持有到下次换仓退出
- 最新实验：`20260426-013801__4h__lb24_bot3_h12_rb12_md5` 已产出最小 account prototype（4h，lookback_bars=24，bottom_k=3，hold_bars=12，rebalance_interval=12，min_drop_pct=5.0）
- 当前观察：这个方向在大多数 1d / 4h 首轮参数上都明显为负，说明“直接买最弱者等反弹”在手续费后很脆弱。当前方向内最佳结果来自 4h 配置 `lb24/bot3/h12/rb12/md5`：年化 `8.5137%`、最大回撤 `55.3162%`、return/drawdown `0.1539`、`2148` 笔 closed trades、total return `102.4951%`。它虽然好于本方向其它配置并保持了相对可控的回撤，但仍明显弱于当前非五浪主力候选（Momentum+Volatility `0.7528`、Cross-sectional Relative Strength `0.5007`、EMA `0.4612`）以及 Donchian 4h baseline（`0.2229`）。结论是：横截面“弱者反弹”目前只算负样本边界探索；若以后继续，应优先引入 regime / liquidity / volume shock 过滤，而不是继续裸刷参数。

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

### 12. Absolute Momentum Gate on Momentum + Volatility Composite
- 类型：复合因子过滤
- 数据需求：OHLCV
- 状态：tested
- 假设：在横截面“动量 ÷ 波动率”排序前，先要求自身绝对动量为正，可减少趋势衰竭与噪声反弹带来的误选
- 最小实现：Momentum + Volatility Composite 排序 + absolute momentum threshold gate + 固定周期轮动退出
- 最新实验：`20260426-033314__1d__lb60_vw60_top5_h5_rb5_mv0p5_am5` 已产出最小 account prototype（1d，lookback_bars=60，volatility_window=60，top_k=5，hold_bars=5，rebalance_interval=5，min_volatility_pct=0.5，min_momentum_pct=5.0）
- 当前观察：这个 gate 明显优于未加 gate 的同源 best（Momentum + Volatility Composite `20260413-204337__1d__lb60_vw60_top5_h5_rb5_mv0p5`，return/drawdown `0.7528`）。当前最佳 am5 配置年化 `56.4906%`、最大回撤 `51.0655%`、return/drawdown `1.1062`、`1196` 笔 closed trades、total return `4674.3019%`；它不仅成为当前最强的非五浪结果，也超过 five-wave 30m trailing-stop 次优结果（`1.0916`）。额外补验的 `am7.5` 配置回落到 `0.8803`，说明 gate 有效，但阈值过高会开始牺牲组合广度。下一步优先考虑 turnover / liquidity 约束、权重方案与更明确的 regime gate，而不是继续裸刷阈值。

### 13. Breadth Regime Gate on Absolute Momentum Gated Composite
- 类型：复合因子过滤
- 数据需求：多币种 OHLCV
- 状态：tested
- 假设：对当前最强的非五浪方向（Absolute Momentum Gated Composite）叠加“市场广度足够健康才参与”的 regime gate，可减少大面积趋势衰竭阶段的回撤暴露。
- 最小实现：沿用 `lb60/vw60/top5/h5/rb5/mv0.5/am5` 复合排序，只在过去 60 bars 内“收益 >= 0%”的币种占比达到阈值时才执行 rebalance。
- 最新实验：`20260426-135011__1d__lb60_vw60_top5_h5_rb5_mv0p5_am5_bf0_br0p4` 已产出最小 account prototype（1d，breadth_floor=0%，min_breadth_ratio=40%）
- 当前观察：breadth gate 的确压低了回撤：最佳首轮配置 `bf0/br0.4` 年化 `40.7029%`、最大回撤 `49.0827%`、return/drawdown `0.8293`、`796` 笔 closed trades、`100` 个币种参与；相较 ungated 的 Absolute Momentum Gated Composite best（`1.1062`）明显退化，但仍强于未加 absolute momentum gate 的 Momentum + Volatility Composite（`0.7528`）与 Cross-sectional Relative Strength（`0.5007`）。结论是：简单 breadth regime gate 可以稳定压回撤，但当前实现削弱了收益弹性，暂时更像“稳健化分支”而不是新的主力 alpha。下一步若继续，应优先试更平滑的 regime 定义（EW universe trend / breadth EMA / drawdown-aware exposure scaling），而不是继续硬阈值裸刷。

### 14. Liquidity-Screened Absolute Momentum Gated Composite
- 类型：复合因子过滤
- 数据需求：多币种 OHLCV
- 状态：tested
- 假设：在当前最强的非五浪方向（Absolute Momentum Gated Composite）上加一个基于 rolling dollar volume 的流动性筛选，可能通过剔除成交稀薄的小币种来减少回撤与手续费敏感性。
- 最小实现：沿用 `lb60/vw60/top5/h5/rb5/mv0.5/am5` 复合排序，只允许进入过去 `liquidity_window` 内 rolling median dollar volume 位于全市场前 `liquidity_universe_ratio` 的币种。
- 最新实验：`20260426-154900__1d__lb60_vw60_lw20_top5_h5_rb5_mv0p5_am5_lr0p9` 已产出最小 account prototype（1d，liquidity_window=20，liquidity_universe_ratio=90%）
- 当前观察：这个方向首轮被基本证伪。实现 sanity check 显示 `lr1.0` 可以近似复现父策略 best（`20260426-154839__..._lr1`，return/drawdown `1.1077`，与父策略 `1.1062` 基本一致），说明脚手架本身没有明显跑偏；但一旦真正开始做流动性收缩，表现就单调退化。当前“真正 screened”里的最佳结果是 `lw20/lr0.9`：年化 `48.4402%`、最大回撤 `57.5085%`、return/drawdown `0.8423`、`1179` 笔 closed trades、avg liquidity-eligible universe `43.3499`。继续收紧后退化更明显：`lr0.5` 已掉到 `0.1760`，`lr0.6`/`lr0.7`/`lr0.8` 也分别只有 `0.1495`~`0.6353`。结论是：这条 alpha 当前并不来自“只做最液体币”子集，硬流动性筛选会同时损伤收益并放大回撤；若后续继续，应优先试更软的做法（exposure scaling / 显式 slippage 模型 / liquidity-aware weighting），而不是继续硬砍 universe。

### 15. Breadth-Scaled Absolute Momentum Composite
- 类型：复合因子过滤 / soft regime scaling
- 数据需求：多币种 OHLCV
- 状态：tested
- 假设：对当前最强非五浪方向（Absolute Momentum Gated Composite）来说，市场广度有信息量，但 hard gate 会过度牺牲收益；改成“按广度动态缩放持仓数量”的软处理，可能在保留大部分上涨弹性的同时显著压低回撤。
- 最小实现：沿用 `lb60/vw60/top5/h5/rb5/mv0.5/am5` 复合排序，用过去 60 bars 内“收益 >= breadth_floor” 的币种占比作为 breadth proxy，并按 breadth ratio 线性缩放当期 `top_k`，而不是整次 rebalance 全跳过。
- 最新实验：`20260426-175037__1d__lb60_vw60_top5_h5_rb5_mv0p5_am5_bf0_bsf0p15` 已产出最小 account prototype（1d，breadth_floor=0%，breadth_scale_floor_ratio=15%）
- 当前观察：这个方向首轮结果比 hard gate 更强，也优于父策略。最佳配置 `bf0/bsf0.15` 年化 `47.4016%`、最大回撤 `35.8483%`、return/drawdown `1.3223`、`629` 笔 closed trades、avg exposure scale `36.7347%`；它明显优于 ungated Absolute Momentum Gated Composite best（`1.1062`）和 Breadth Regime Gated Composite best（`0.8293`），说明“广度有用，但应该做成连续缩放而不是一刀切 gate”。首轮邻域里 `bf0/bsf0.2` 也有 `1.1182`，而更高 floor（如 `bf2.5/bsf0.2=1.1765`）虽也强，但不如 `bf0/bsf0.15`。当前结论是：soft breadth scaling 是目前最有希望的非五浪稳健化分支之一。
- 下一步优先：不要急着继续细抠单点阈值；优先验证更连续的实现（breadth EMA / explicit exposure weights / drawdown-aware scaling）以及加入 turnover/slippage 约束，确认优势不是单纯来自低曝光。

### 16. Pullback Entry in Trend
- 类型：趋势回撤入场
- 数据需求：OHLCV
- 假设：趋势中的回撤买点比追突破有更好盈亏比
- 最小实现：趋势过滤 + 回撤到均线/通道中位后再入场

### 17. Inverse Short Absolute Momentum Gated Composite
- 类型：反向验证 / 空头对照
- 数据需求：多币种 OHLCV
- 状态：tested
- 假设：如果 Absolute Momentum Gated Composite 的信号同时适合做空，可能说明它只是捕捉高波动而不是方向性 alpha；若反向显著亏损，则更支持原策略方向性有效。
- 最小实现：沿用 `lb60/vw60/top5/h5/rb5/mv0.5/am5` 选币与退出信号，但账户执行改为 `sell_short` 入场、`buy_to_cover` 退出，逐日按空头负债盯市，手续费与原 account 回测一致。
- 最新实验：`20260426-182224__1d__short_lb60_vw60_top5_h5_rb5_mv0p5_am5` 已产出 short account prototype（1d，lookback_bars=60，volatility_window=60，top_k=5，hold_bars=5，rebalance_interval=5，min_volatility_pct=0.5，min_momentum_pct=5.0）
- 当前观察：反向做空基本被证伪：年化 `-58.7051%`、最大回撤 `99.9620%`、return/drawdown `-0.5873`、`1188` 笔 closed trades、total return `-99.9540%`。胜率 `49.5791%` 接近随机，但平均单笔 `-2.5818%`、最差单笔 `-157.0160%`，说明原信号不是“多空两边都能赚”的泛高波动选择器，而是更偏趋势延续方向性 alpha；空头版不进入活跃榜单。注意：当前 short account 未计借币费/资金费，真实做空成本只会让结果更差。

### 18. Paired Logical Mirror Short Ranking
- 类型：全策略反选做空验证 / 排名规则
- 数据需求：历史 account 回测 + OHLCV
- 状态：tested
- 假设：最终有效策略应同时看正向结果和逻辑镜像反向做空结果；本轮按用户指定的原始均值 `paired=(forward return/drawdown + inverse short return/drawdown)/2` 排名。
- 最小实现：新增统一 logical mirror short signal generator，覆盖横截面排序、Donchian、EMA、Z-score、Volatility Compression、Five-wave；用 short account 执行；批量入口 `coin_research.rank_paired_inverse_short` 扫描历史正向 account run，生成 `*-inverse-short` artifacts，并重写 active top 10 leaderboard。
- 最新实验：`reports/backtests/paired-inverse-short-ranking/20260426-200748/ranking.json` 已完成全历史配对重排：`79` 条正向 account run 均生成 inverse short artifacts，`78` 条可计算 paired score，`0` blocked；唯一未入排名的是 `20260406-201604__1d__three_wave_exit`，因为正向 summary 缺少可计算 return/drawdown。
- 当前观察：按“原始均值”规则，five-wave 镜像 short 结果显著为正，导致 5m/30m five-wave 仍占据前三；当前第 4 名为 Breadth-Scaled Absolute Momentum Composite `bf0/bsf0.15`，paired `0.5687`（forward `1.3223`，inverse `-0.1848`）。这个规则会奖励“正向和反向镜像都赚钱”的结构，和此前“反向亏损证明方向性”的解释不同；后续若目标是筛方向性 alpha，应考虑改用 `(forward - inverse_short)/2` 或单独展示 robustness score。

## Working rules

- 每个 2 小时实验周期优先从这里挑一个方向推进
- 若没有实现条件，先补最小研究脚手架或先写清实验设计
- 每次推进后更新状态：`idea` / `prototype` / `tested` / `promoted`
- 只有进入活跃前 10 的结果，才更新 `research/leaderboard.json`
