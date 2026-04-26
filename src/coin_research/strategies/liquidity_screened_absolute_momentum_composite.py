from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from math import ceil
from typing import Any

import pandas as pd


DEFAULT_LOOKBACK_BARS = 60
DEFAULT_VOLATILITY_WINDOW = 20
DEFAULT_LIQUIDITY_WINDOW = 20
DEFAULT_HOLD_BARS = 10
DEFAULT_TOP_K = 5
DEFAULT_REBALANCE_INTERVAL = 10
DEFAULT_MIN_VOLATILITY_PCT = 0.5
DEFAULT_MIN_MOMENTUM_PCT = 5.0
DEFAULT_LIQUIDITY_UNIVERSE_RATIO = 0.5
EXIT_REASON_REBALANCE = "rebalance_exit"


@dataclass(frozen=True)
class LiquidityScreenedAbsoluteMomentumCompositeTrade:
    signal_id: str
    symbol: str
    wave_start_date: Any
    wave_end_date: Any
    p0_price: float
    p1_price: float
    p2_price: float
    p3_price: float
    p4_price: float
    p5_price: float
    p0_date: Any
    p1_date: Any
    p2_date: Any
    p3_date: Any
    p4_date: Any
    p5_date: Any
    wave_drop_pct: float
    speed1: float
    speed3: float
    speed5: float
    fractal_center_date: Any
    signal_confirm_date: Any
    entry_date: Any
    entry_price: float
    planned_hold_bars: int
    exit_date: Any | None
    exit_price: float | None
    return_pct: float | None
    holding_days: int | None
    status: str
    exit_reason: str | None
    p0_index: int
    p5_index: int
    entry_index: int
    exit_index: int | None

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record.pop("p0_index")
        record.pop("p5_index")
        record.pop("entry_index")
        record.pop("exit_index")
        return record


@dataclass(frozen=True)
class LiquidityScreenedAbsoluteMomentumCompositeResult:
    trades: list[LiquidityScreenedAbsoluteMomentumCompositeTrade]
    evaluated_rebalance_count: int
    avg_liquidity_eligible_universe_size: float | None


def _format_signal_time(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y%m%dT%H%M%S")
    if isinstance(value, datetime):
        return value.strftime("%Y%m%dT%H%M%S")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _prepare_frame(df: pd.DataFrame, *, time_column: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    required = {time_column, "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")
    frame = df.loc[:, [column for column in df.columns if column in required or column in {"symbol", "source", "fetched_at"}]].copy()
    frame[time_column] = pd.to_datetime(frame[time_column], errors="coerce", utc=True)
    for column in ("open", "high", "low", "close", "volume"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame[(frame["open"] > 0) & (frame["high"] > 0) & (frame["low"] > 0) & (frame["close"] > 0) & (frame["volume"] > 0)]
    frame = frame.dropna(subset=[time_column, "open", "high", "low", "close", "volume"]).sort_values(time_column).reset_index(drop=True)
    return frame


def _validate_params(
    *,
    lookback_bars: int,
    volatility_window: int,
    liquidity_window: int,
    hold_bars: int,
    top_k: int,
    rebalance_interval: int,
    min_universe_size: int | None,
    min_volatility_pct: float,
    min_momentum_pct: float,
    liquidity_universe_ratio: float,
) -> int:
    if lookback_bars <= 0:
        raise ValueError(f"lookback_bars must be > 0, got {lookback_bars}")
    if volatility_window <= 1:
        raise ValueError(f"volatility_window must be > 1, got {volatility_window}")
    if liquidity_window <= 1:
        raise ValueError(f"liquidity_window must be > 1, got {liquidity_window}")
    if hold_bars <= 0:
        raise ValueError(f"hold_bars must be > 0, got {hold_bars}")
    if top_k <= 0:
        raise ValueError(f"top_k must be > 0, got {top_k}")
    if rebalance_interval <= 0:
        raise ValueError(f"rebalance_interval must be > 0, got {rebalance_interval}")
    if min_volatility_pct <= 0:
        raise ValueError(f"min_volatility_pct must be > 0, got {min_volatility_pct}")
    if min_momentum_pct < 0:
        raise ValueError(f"min_momentum_pct must be >= 0, got {min_momentum_pct}")
    if not 0.0 < liquidity_universe_ratio <= 1.0:
        raise ValueError(f"liquidity_universe_ratio must be within (0, 1], got {liquidity_universe_ratio}")
    required_universe = min_universe_size if min_universe_size is not None else top_k
    if required_universe < top_k:
        raise ValueError(f"min_universe_size must be >= top_k, got min_universe_size={required_universe}, top_k={top_k}")
    return required_universe


def summarize_trade_results(
    result: LiquidityScreenedAbsoluteMomentumCompositeResult,
    *,
    universe_symbols: int,
    rebalance_interval: int,
    top_k: int,
    volatility_window: int,
    liquidity_window: int,
    min_momentum_pct: float,
    liquidity_universe_ratio: float,
) -> dict[str, float | int | None]:
    trades = result.trades
    closed = [trade for trade in trades if trade.status == "closed" and trade.return_pct is not None]
    returns = pd.Series([float(trade.return_pct) for trade in closed], dtype="float64")
    holding_bars = pd.Series([float(trade.holding_days or 0) for trade in closed], dtype="float64")
    rebalance_count = len({trade.signal_confirm_date for trade in trades})
    score_series = pd.Series([float(trade.speed5) for trade in trades], dtype="float64") if trades else pd.Series(dtype="float64")
    liquidity_series = pd.Series([float(trade.speed3) for trade in trades], dtype="float64") if trades else pd.Series(dtype="float64")
    volatility_series = pd.Series([float(trade.p5_price) for trade in trades], dtype="float64") if trades else pd.Series(dtype="float64")
    momentum_series = pd.Series([float(trade.wave_drop_pct) for trade in trades], dtype="float64") if trades else pd.Series(dtype="float64")
    return {
        "universe_symbols": universe_symbols,
        "rebalance_interval": rebalance_interval,
        "top_k": top_k,
        "volatility_window": volatility_window,
        "liquidity_window": liquidity_window,
        "min_momentum_pct": min_momentum_pct,
        "liquidity_universe_ratio": liquidity_universe_ratio,
        "evaluated_rebalance_count": result.evaluated_rebalance_count,
        "avg_liquidity_eligible_universe_size": result.avg_liquidity_eligible_universe_size,
        "rebalance_count": rebalance_count,
        "signals_found": len(trades),
        "closed_trades": len(closed),
        "incomplete_trades": len(trades) - len(closed),
        "win_rate": round(float((returns > 0).mean() * 100.0), 4) if not returns.empty else None,
        "avg_return_pct": round(float(returns.mean()), 4) if not returns.empty else None,
        "median_return_pct": round(float(returns.median()), 4) if not returns.empty else None,
        "avg_holding_bars": round(float(holding_bars.mean()), 4) if not holding_bars.empty else None,
        "avg_holding_days": round(float(holding_bars.mean()), 4) if not holding_bars.empty else None,
        "best_trade_pct": round(float(returns.max()), 4) if not returns.empty else None,
        "worst_trade_pct": round(float(returns.min()), 4) if not returns.empty else None,
        "avg_score": round(float(score_series.mean()), 4) if not score_series.empty else None,
        "avg_selected_median_dollar_volume": round(float(liquidity_series.mean()), 4) if not liquidity_series.empty else None,
        "avg_selected_volatility_pct": round(float(volatility_series.mean()), 4) if not volatility_series.empty else None,
        "avg_selected_momentum_pct": round(float(momentum_series.mean()), 4) if not momentum_series.empty else None,
        "rebalance_exit_count": sum(1 for trade in closed if trade.exit_reason == EXIT_REASON_REBALANCE),
    }


def run_liquidity_screened_absolute_momentum_composite_backtest(
    market_frames: dict[str, pd.DataFrame],
    *,
    lookback_bars: int = DEFAULT_LOOKBACK_BARS,
    volatility_window: int = DEFAULT_VOLATILITY_WINDOW,
    liquidity_window: int = DEFAULT_LIQUIDITY_WINDOW,
    hold_bars: int = DEFAULT_HOLD_BARS,
    top_k: int = DEFAULT_TOP_K,
    rebalance_interval: int = DEFAULT_REBALANCE_INTERVAL,
    min_universe_size: int | None = None,
    min_volatility_pct: float = DEFAULT_MIN_VOLATILITY_PCT,
    min_momentum_pct: float = DEFAULT_MIN_MOMENTUM_PCT,
    liquidity_universe_ratio: float = DEFAULT_LIQUIDITY_UNIVERSE_RATIO,
    time_column: str = "bar_time",
    enforce_non_overlapping: bool = True,
) -> LiquidityScreenedAbsoluteMomentumCompositeResult:
    required_universe = _validate_params(
        lookback_bars=lookback_bars,
        volatility_window=volatility_window,
        liquidity_window=liquidity_window,
        hold_bars=hold_bars,
        top_k=top_k,
        rebalance_interval=rebalance_interval,
        min_universe_size=min_universe_size,
        min_volatility_pct=min_volatility_pct,
        min_momentum_pct=min_momentum_pct,
        liquidity_universe_ratio=liquidity_universe_ratio,
    )

    prepared_frames: dict[str, pd.DataFrame] = {}
    for symbol, frame in market_frames.items():
        prepared = _prepare_frame(frame, time_column=time_column)
        if prepared.empty:
            continue
        prepared_frames[symbol] = prepared
    if not prepared_frames:
        return LiquidityScreenedAbsoluteMomentumCompositeResult(
            trades=[],
            evaluated_rebalance_count=0,
            avg_liquidity_eligible_universe_size=None,
        )

    close_wide = pd.concat(
        [frame.set_index(time_column)["close"].rename(symbol) for symbol, frame in prepared_frames.items()],
        axis=1,
    ).sort_index()
    open_wide = pd.concat(
        [frame.set_index(time_column)["open"].rename(symbol) for symbol, frame in prepared_frames.items()],
        axis=1,
    ).sort_index()
    volume_wide = pd.concat(
        [frame.set_index(time_column)["volume"].rename(symbol) for symbol, frame in prepared_frames.items()],
        axis=1,
    ).sort_index()
    returns_wide = close_wide.pct_change() * 100.0
    dollar_volume_wide = close_wide * volume_wide
    rolling_median_dollar_volume = dollar_volume_wide.rolling(liquidity_window).median()
    timeline = list(close_wide.index)
    start_index = max(lookback_bars, volatility_window, liquidity_window)
    if len(timeline) <= start_index + 1:
        return LiquidityScreenedAbsoluteMomentumCompositeResult(
            trades=[],
            evaluated_rebalance_count=0,
            avg_liquidity_eligible_universe_size=None,
        )

    trades: list[LiquidityScreenedAbsoluteMomentumCompositeTrade] = []
    next_available_entry_idx: dict[str, int] = {}
    evaluated_rebalance_count = 0
    liquidity_eligible_sizes: list[int] = []

    for signal_index in range(start_index, len(timeline) - 1, rebalance_interval):
        signal_time = timeline[signal_index]
        lookback_time = timeline[signal_index - lookback_bars]
        entry_index = signal_index + 1
        if entry_index >= len(timeline):
            break

        ranking_close = close_wide.iloc[signal_index]
        lookback_close = close_wide.iloc[signal_index - lookback_bars]
        momentum = ((ranking_close / lookback_close) - 1.0) * 100.0
        realized_volatility = returns_wide.rolling(volatility_window).std().iloc[signal_index]
        median_dollar_volume = rolling_median_dollar_volume.iloc[signal_index]
        metrics_frame = pd.DataFrame(
            {
                "momentum_pct": momentum,
                "volatility_pct": realized_volatility,
                "median_dollar_volume": median_dollar_volume,
            }
        )
        metrics_frame = metrics_frame.replace([pd.NA, pd.NaT, float("inf"), float("-inf")], pd.NA).dropna()
        if len(metrics_frame) < required_universe:
            continue

        evaluated_rebalance_count += 1
        liquidity_eligible_count = max(required_universe, int(ceil(len(metrics_frame) * liquidity_universe_ratio)))
        liquidity_frame = metrics_frame.sort_values(["median_dollar_volume", "momentum_pct"], ascending=False).head(liquidity_eligible_count)
        liquidity_eligible_sizes.append(len(liquidity_frame))

        ranking_frame = liquidity_frame[liquidity_frame["momentum_pct"] >= min_momentum_pct].copy()
        if len(ranking_frame) < required_universe:
            continue

        ranking_frame["volatility_pct"] = ranking_frame["volatility_pct"].clip(lower=min_volatility_pct)
        ranking_frame["score"] = ranking_frame["momentum_pct"] / ranking_frame["volatility_pct"]
        selected = ranking_frame.sort_values(["score", "momentum_pct", "median_dollar_volume"], ascending=False).head(top_k)
        if selected.empty:
            continue

        for rank, (symbol, row) in enumerate(selected.iterrows(), start=1):
            if enforce_non_overlapping and entry_index <= next_available_entry_idx.get(symbol, -1):
                continue

            entry_price = open_wide.iloc[entry_index].get(symbol)
            if pd.isna(entry_price) or float(entry_price) <= 0:
                continue

            planned_exit_index = entry_index + hold_bars
            if planned_exit_index < len(timeline):
                exit_index = planned_exit_index
                exit_time = timeline[exit_index]
                exit_price = open_wide.iloc[exit_index].get(symbol)
                if pd.isna(exit_price) or float(exit_price) <= 0:
                    exit_index = None
                    exit_time = None
                    exit_price = None
            else:
                exit_index = None
                exit_time = None
                exit_price = None

            if exit_index is None:
                status = "incomplete"
                exit_reason = None
                holding_bars_count = None
                return_pct = None
                next_available_entry_idx[symbol] = len(timeline) - 1 if enforce_non_overlapping else next_available_entry_idx.get(symbol, -1)
            else:
                status = "closed"
                exit_reason = EXIT_REASON_REBALANCE
                holding_bars_count = hold_bars
                return_pct = ((float(exit_price) / float(entry_price)) - 1.0) * 100.0
                next_available_entry_idx[symbol] = exit_index

            trades.append(
                LiquidityScreenedAbsoluteMomentumCompositeTrade(
                    signal_id=(
                        f"{symbol}-{_format_signal_time(signal_time)}-"
                        f"lb{lookback_bars}-vw{volatility_window}-lw{liquidity_window}-top{top_k}-"
                        f"h{hold_bars}-am{min_momentum_pct}-lr{liquidity_universe_ratio}-r{rank}"
                    ),
                    symbol=symbol,
                    wave_start_date=lookback_time,
                    wave_end_date=signal_time,
                    p0_price=round(float(lookback_close[symbol]), 6),
                    p1_price=round(float(ranking_close[symbol]), 6),
                    p2_price=round(float(entry_price), 6),
                    p3_price=round(float(exit_price), 6) if exit_price is not None else 0.0,
                    p4_price=round(float(row["momentum_pct"]), 6),
                    p5_price=round(float(row["volatility_pct"]), 6),
                    p0_date=lookback_time,
                    p1_date=signal_time,
                    p2_date=timeline[entry_index],
                    p3_date=exit_time,
                    p4_date=None,
                    p5_date=None,
                    wave_drop_pct=round(float(row["momentum_pct"]), 4),
                    speed1=float(rank),
                    speed3=round(float(row["median_dollar_volume"]), 6),
                    speed5=round(float(row["score"]), 6),
                    fractal_center_date=signal_time,
                    signal_confirm_date=signal_time,
                    entry_date=timeline[entry_index],
                    entry_price=round(float(entry_price), 6),
                    planned_hold_bars=hold_bars,
                    exit_date=exit_time,
                    exit_price=round(float(exit_price), 6) if exit_price is not None else None,
                    return_pct=round(float(return_pct), 4) if return_pct is not None else None,
                    holding_days=holding_bars_count,
                    status=status,
                    exit_reason=exit_reason,
                    p0_index=signal_index - lookback_bars,
                    p5_index=signal_index,
                    entry_index=entry_index,
                    exit_index=exit_index,
                )
            )

    avg_liquidity_eligible_universe_size = (
        round(float(pd.Series(liquidity_eligible_sizes, dtype="float64").mean()), 4)
        if liquidity_eligible_sizes
        else None
    )
    return LiquidityScreenedAbsoluteMomentumCompositeResult(
        trades=trades,
        evaluated_rebalance_count=evaluated_rebalance_count,
        avg_liquidity_eligible_universe_size=avg_liquidity_eligible_universe_size,
    )


__all__ = [
    "DEFAULT_HOLD_BARS",
    "DEFAULT_LIQUIDITY_UNIVERSE_RATIO",
    "DEFAULT_LIQUIDITY_WINDOW",
    "DEFAULT_LOOKBACK_BARS",
    "DEFAULT_MIN_MOMENTUM_PCT",
    "DEFAULT_MIN_VOLATILITY_PCT",
    "DEFAULT_REBALANCE_INTERVAL",
    "DEFAULT_TOP_K",
    "DEFAULT_VOLATILITY_WINDOW",
    "EXIT_REASON_REBALANCE",
    "LiquidityScreenedAbsoluteMomentumCompositeResult",
    "LiquidityScreenedAbsoluteMomentumCompositeTrade",
    "run_liquidity_screened_absolute_momentum_composite_backtest",
    "summarize_trade_results",
]
