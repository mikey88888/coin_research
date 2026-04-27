from __future__ import annotations

from typing import Any

import pandas as pd

from .breadth_scaled_absolute_momentum_composite import (
    EXIT_REASON_REBALANCE,
    BreadthScaledAbsoluteMomentumCompositeResult,
    BreadthScaledAbsoluteMomentumCompositeTrade,
    DEFAULT_BREADTH_MOMENTUM_FLOOR_PCT,
    DEFAULT_BREADTH_SCALE_FLOOR_RATIO,
    DEFAULT_HOLD_BARS,
    DEFAULT_LOOKBACK_BARS,
    DEFAULT_MIN_MOMENTUM_PCT,
    DEFAULT_MIN_VOLATILITY_PCT,
    DEFAULT_REBALANCE_INTERVAL,
    DEFAULT_TOP_K,
    DEFAULT_VOLATILITY_WINDOW,
    _breadth_scale,
    _format_signal_time,
    _prepare_frame,
    _validate_params,
    summarize_trade_results,
)


DEFAULT_BREADTH_EMA_SPAN = 3


def _validate_breadth_ema_span(breadth_ema_span: int) -> None:
    if breadth_ema_span <= 0:
        raise ValueError(f"breadth_ema_span must be > 0, got {breadth_ema_span}")


def run_breadth_ema_scaled_absolute_momentum_composite_backtest(
    market_frames: dict[str, pd.DataFrame],
    *,
    lookback_bars: int = DEFAULT_LOOKBACK_BARS,
    volatility_window: int = DEFAULT_VOLATILITY_WINDOW,
    hold_bars: int = DEFAULT_HOLD_BARS,
    top_k: int = DEFAULT_TOP_K,
    rebalance_interval: int = DEFAULT_REBALANCE_INTERVAL,
    min_universe_size: int | None = None,
    min_volatility_pct: float = DEFAULT_MIN_VOLATILITY_PCT,
    min_momentum_pct: float = DEFAULT_MIN_MOMENTUM_PCT,
    breadth_momentum_floor_pct: float = DEFAULT_BREADTH_MOMENTUM_FLOOR_PCT,
    breadth_scale_floor_ratio: float = DEFAULT_BREADTH_SCALE_FLOOR_RATIO,
    breadth_ema_span: int = DEFAULT_BREADTH_EMA_SPAN,
    time_column: str = "bar_time",
    enforce_non_overlapping: bool = True,
) -> BreadthScaledAbsoluteMomentumCompositeResult:
    required_universe = _validate_params(
        lookback_bars=lookback_bars,
        volatility_window=volatility_window,
        hold_bars=hold_bars,
        top_k=top_k,
        rebalance_interval=rebalance_interval,
        min_universe_size=min_universe_size,
        min_volatility_pct=min_volatility_pct,
        min_momentum_pct=min_momentum_pct,
        breadth_momentum_floor_pct=breadth_momentum_floor_pct,
        breadth_scale_floor_ratio=breadth_scale_floor_ratio,
    )
    _validate_breadth_ema_span(breadth_ema_span)

    prepared_frames: dict[str, pd.DataFrame] = {}
    for symbol, frame in market_frames.items():
        prepared = _prepare_frame(frame, time_column=time_column)
        if prepared.empty:
            continue
        prepared_frames[symbol] = prepared
    if not prepared_frames:
        return BreadthScaledAbsoluteMomentumCompositeResult(
            trades=[],
            evaluated_rebalance_count=0,
            selected_rebalance_count=0,
            skipped_for_scale_zero_rebalance_count=0,
        )

    close_wide = pd.concat(
        [frame.set_index(time_column)["close"].rename(symbol) for symbol, frame in prepared_frames.items()],
        axis=1,
    ).sort_index()
    open_wide = pd.concat(
        [frame.set_index(time_column)["open"].rename(symbol) for symbol, frame in prepared_frames.items()],
        axis=1,
    ).sort_index()
    returns_wide = close_wide.pct_change() * 100.0
    momentum_wide = ((close_wide / close_wide.shift(lookback_bars)) - 1.0) * 100.0
    realized_volatility_wide = returns_wide.rolling(volatility_window).std()
    valid_mask = momentum_wide.notna() & realized_volatility_wide.notna()
    raw_breadth_series = (
        momentum_wide.ge(breadth_momentum_floor_pct).where(valid_mask).mean(axis=1, skipna=True)
    )
    smoothed_breadth_series = raw_breadth_series.ewm(span=breadth_ema_span, adjust=False, min_periods=1).mean()

    timeline = list(close_wide.index)
    start_index = max(lookback_bars, volatility_window)
    if len(timeline) <= start_index + 1:
        return BreadthScaledAbsoluteMomentumCompositeResult(
            trades=[],
            evaluated_rebalance_count=0,
            selected_rebalance_count=0,
            skipped_for_scale_zero_rebalance_count=0,
        )

    trades: list[BreadthScaledAbsoluteMomentumCompositeTrade] = []
    next_available_entry_idx: dict[str, int] = {}
    evaluated_rebalance_count = 0
    selected_rebalance_count = 0
    skipped_for_scale_zero_rebalance_count = 0

    for signal_index in range(start_index, len(timeline) - 1, rebalance_interval):
        signal_time = timeline[signal_index]
        lookback_time = timeline[signal_index - lookback_bars]
        entry_index = signal_index + 1
        if entry_index >= len(timeline):
            break

        ranking_close = close_wide.iloc[signal_index]
        lookback_close = close_wide.iloc[signal_index - lookback_bars]
        momentum = momentum_wide.iloc[signal_index]
        realized_volatility = realized_volatility_wide.iloc[signal_index]
        metrics_frame = pd.DataFrame(
            {
                "momentum_pct": momentum,
                "volatility_pct": realized_volatility,
            }
        )
        metrics_frame = metrics_frame.replace([pd.NA, pd.NaT, float("inf"), float("-inf")], pd.NA).dropna()
        if len(metrics_frame) < required_universe:
            continue

        evaluated_rebalance_count += 1
        breadth_ratio = smoothed_breadth_series.iloc[signal_index]
        if pd.isna(breadth_ratio):
            continue
        scale = _breadth_scale(
            breadth_ratio=float(breadth_ratio),
            breadth_scale_floor_ratio=breadth_scale_floor_ratio,
        )
        if scale <= 0:
            skipped_for_scale_zero_rebalance_count += 1
            continue

        ranking_frame = metrics_frame[metrics_frame["momentum_pct"] >= min_momentum_pct].copy()
        if ranking_frame.empty:
            continue

        dynamic_top_k = min(len(ranking_frame), top_k, max(1, int(top_k * scale)))
        if dynamic_top_k <= 0:
            skipped_for_scale_zero_rebalance_count += 1
            continue

        ranking_frame["volatility_pct"] = ranking_frame["volatility_pct"].clip(lower=min_volatility_pct)
        ranking_frame["score"] = ranking_frame["momentum_pct"] / ranking_frame["volatility_pct"]
        selected = ranking_frame.sort_values(["score", "momentum_pct"], ascending=False).head(dynamic_top_k)
        if selected.empty:
            continue

        selected_rebalance_count += 1
        breadth_pct = float(breadth_ratio) * 100.0
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
                next_available_entry_idx[symbol] = (
                    len(timeline) - 1 if enforce_non_overlapping else next_available_entry_idx.get(symbol, -1)
                )
            else:
                status = "closed"
                exit_reason = EXIT_REASON_REBALANCE
                holding_bars_count = hold_bars
                return_pct = ((float(exit_price) / float(entry_price)) - 1.0) * 100.0
                next_available_entry_idx[symbol] = exit_index

            trades.append(
                BreadthScaledAbsoluteMomentumCompositeTrade(
                    signal_id=(
                        f"{symbol}-{_format_signal_time(signal_time)}-"
                        f"lb{lookback_bars}-vw{volatility_window}-top{top_k}-"
                        f"h{hold_bars}-am{min_momentum_pct}-bf{breadth_momentum_floor_pct}-"
                        f"bsf{breadth_scale_floor_ratio}-be{breadth_ema_span}-r{rank}"
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
                    speed3=round(float(breadth_pct), 4),
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

    return BreadthScaledAbsoluteMomentumCompositeResult(
        trades=trades,
        evaluated_rebalance_count=evaluated_rebalance_count,
        selected_rebalance_count=selected_rebalance_count,
        skipped_for_scale_zero_rebalance_count=skipped_for_scale_zero_rebalance_count,
    )


__all__ = [
    "BreadthScaledAbsoluteMomentumCompositeResult",
    "BreadthScaledAbsoluteMomentumCompositeTrade",
    "DEFAULT_BREADTH_EMA_SPAN",
    "DEFAULT_BREADTH_MOMENTUM_FLOOR_PCT",
    "DEFAULT_BREADTH_SCALE_FLOOR_RATIO",
    "DEFAULT_HOLD_BARS",
    "DEFAULT_LOOKBACK_BARS",
    "DEFAULT_MIN_MOMENTUM_PCT",
    "DEFAULT_MIN_VOLATILITY_PCT",
    "DEFAULT_REBALANCE_INTERVAL",
    "DEFAULT_TOP_K",
    "DEFAULT_VOLATILITY_WINDOW",
    "EXIT_REASON_REBALANCE",
    "run_breadth_ema_scaled_absolute_momentum_composite_backtest",
    "summarize_trade_results",
]
