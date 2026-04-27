from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd

from .strategies.five_wave_reversal import (
    DEFAULT_TRAILING_STOP_PCT,
    EXIT_MODE_THREE_WAVE,
    EXIT_MODE_TIME_ONLY,
    EXIT_MODE_TRAILING_STOP,
    MIN_WAVE_BAR_COUNT,
    MIN_WAVE_DROP_PCT,
    REVERSAL_PCT,
    Pivot,
    _bars_between,
    _bars_in_wave,
    _find_bottom_fractal,
    _find_top_fractal,
    _map_pivots_to_global,
    build_zigzag_pivots,
)


EXIT_REASON_REBALANCE = "rebalance_exit"
INVERSE_DEFINITION = "logical_mirror_short"


@dataclass(frozen=True)
class InverseShortSignal:
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
        record = dict(self.__dict__)
        record.pop("p0_index")
        record.pop("p5_index")
        record.pop("entry_index")
        record.pop("exit_index")
        return record


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
    required = {time_column, "open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")
    frame = df.loc[:, [column for column in df.columns if column in required or column in {"symbol", "volume", "source", "fetched_at"}]].copy()
    frame[time_column] = pd.to_datetime(frame[time_column], errors="coerce", utc=True)
    for column in ("open", "high", "low", "close"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "volume" in frame.columns:
        frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame = frame[(frame["open"] > 0) & (frame["high"] > 0) & (frame["low"] > 0) & (frame["close"] > 0)]
    return frame.dropna(subset=[time_column, "open", "high", "low", "close"]).sort_values(time_column).reset_index(drop=True)


def _wide_frames(market_frames: dict[str, pd.DataFrame], *, time_column: str) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    prepared = {symbol: _prepare_frame(frame, time_column=time_column) for symbol, frame in market_frames.items()}
    prepared = {symbol: frame for symbol, frame in prepared.items() if not frame.empty}
    if not prepared:
        return {}, pd.DataFrame(), pd.DataFrame()
    close_wide = pd.concat([frame.set_index(time_column)["close"].rename(symbol) for symbol, frame in prepared.items()], axis=1).sort_index()
    open_wide = pd.concat([frame.set_index(time_column)["open"].rename(symbol) for symbol, frame in prepared.items()], axis=1).sort_index()
    return prepared, close_wide, open_wide


def _fixed_horizon_signal(
    *,
    strategy_key: str,
    symbol: str,
    signal_time: Any,
    lookback_time: Any,
    entry_time: Any,
    entry_price: float,
    exit_time: Any | None,
    exit_price: float | None,
    metric: float,
    rank: int,
    candidate_count: int,
    planned_hold_bars: int,
    p0_price: float,
    p1_price: float,
    p0_index: int,
    p5_index: int,
    entry_index: int,
    exit_index: int | None,
    score: float | None = None,
) -> InverseShortSignal:
    return_pct = ((float(exit_price) / float(entry_price)) - 1.0) * 100.0 if exit_price is not None else None
    return InverseShortSignal(
        signal_id=f"{symbol}-{_format_signal_time(signal_time)}-{strategy_key}-inverse-r{rank}",
        symbol=symbol,
        wave_start_date=lookback_time,
        wave_end_date=signal_time,
        p0_price=round(float(p0_price), 6),
        p1_price=round(float(p1_price), 6),
        p2_price=round(float(entry_price), 6),
        p3_price=round(float(exit_price), 6) if exit_price is not None else 0.0,
        p4_price=round(float(metric), 6),
        p5_price=round(float(score if score is not None else candidate_count), 6),
        p0_date=lookback_time,
        p1_date=signal_time,
        p2_date=entry_time,
        p3_date=exit_time,
        p4_date=None,
        p5_date=None,
        wave_drop_pct=round(float(metric), 4),
        speed1=float(rank),
        speed3=float(candidate_count),
        speed5=round(float(score if score is not None else planned_hold_bars), 6),
        fractal_center_date=signal_time,
        signal_confirm_date=signal_time,
        entry_date=entry_time,
        entry_price=round(float(entry_price), 6),
        planned_hold_bars=planned_hold_bars,
        exit_date=exit_time,
        exit_price=round(float(exit_price), 6) if exit_price is not None else None,
        return_pct=round(float(return_pct), 4) if return_pct is not None else None,
        holding_days=planned_hold_bars if exit_index is not None else None,
        status="closed" if exit_index is not None else "incomplete",
        exit_reason=EXIT_REASON_REBALANCE if exit_index is not None else None,
        p0_index=p0_index,
        p5_index=p5_index,
        entry_index=entry_index,
        exit_index=exit_index,
    )


def _build_fixed_horizon_trades(
    *,
    strategy_key: str,
    close_wide: pd.DataFrame,
    open_wide: pd.DataFrame,
    lookback_bars: int,
    hold_bars: int,
    rebalance_interval: int,
    top_k: int,
    min_universe_size: int | None,
    metric_builder: Any,
    selector: Any,
    start_index: int | None = None,
    enforce_non_overlapping: bool = True,
) -> list[InverseShortSignal]:
    timeline = list(close_wide.index)
    first_index = start_index if start_index is not None else lookback_bars
    required_universe = min_universe_size if min_universe_size is not None else top_k
    if len(timeline) <= first_index + 1:
        return []

    trades: list[InverseShortSignal] = []
    next_available_entry_idx: dict[str, int] = {}
    for signal_index in range(first_index, len(timeline) - 1, rebalance_interval):
        signal_time = timeline[signal_index]
        lookback_time = timeline[max(signal_index - lookback_bars, 0)]
        entry_index = signal_index + 1
        metrics = metric_builder(signal_index)
        if metrics.empty or len(metrics) < required_universe:
            continue
        selected = selector(metrics).head(top_k)
        if selected.empty:
            continue

        for rank, (symbol, row) in enumerate(selected.iterrows(), start=1):
            if enforce_non_overlapping and entry_index <= next_available_entry_idx.get(symbol, -1):
                continue
            entry_price = open_wide.iloc[entry_index].get(symbol)
            if pd.isna(entry_price) or float(entry_price) <= 0:
                continue
            exit_index = entry_index + hold_bars
            exit_time = None
            exit_price = None
            if exit_index < len(timeline):
                candidate_exit_price = open_wide.iloc[exit_index].get(symbol)
                if not pd.isna(candidate_exit_price) and float(candidate_exit_price) > 0:
                    exit_time = timeline[exit_index]
                    exit_price = float(candidate_exit_price)
                else:
                    exit_index = None
            else:
                exit_index = None
            if enforce_non_overlapping:
                next_available_entry_idx[symbol] = exit_index if exit_index is not None else len(timeline) - 1
            trades.append(
                _fixed_horizon_signal(
                    strategy_key=strategy_key,
                    symbol=symbol,
                    signal_time=signal_time,
                    lookback_time=lookback_time,
                    entry_time=timeline[entry_index],
                    entry_price=float(entry_price),
                    exit_time=exit_time,
                    exit_price=exit_price,
                    metric=float(row.get("metric", 0.0)),
                    rank=rank,
                    candidate_count=len(metrics),
                    planned_hold_bars=hold_bars,
                    p0_price=float(close_wide.iloc[max(signal_index - lookback_bars, 0)].get(symbol)),
                    p1_price=float(close_wide.iloc[signal_index].get(symbol)),
                    p0_index=max(signal_index - lookback_bars, 0),
                    p5_index=signal_index,
                    entry_index=entry_index,
                    exit_index=exit_index,
                    score=float(row["score"]) if "score" in row and not pd.isna(row["score"]) else None,
                )
            )
    return trades


def _cross_sectional_inverse(strategy_key: str, market_frames: dict[str, pd.DataFrame], params: dict[str, Any], *, time_column: str) -> list[InverseShortSignal]:
    _, close_wide, open_wide = _wide_frames(market_frames, time_column=time_column)
    if close_wide.empty:
        return []
    lookback_bars = int(params.get("lookback_bars", 60))
    hold_bars = int(params.get("hold_bars", 10))
    top_k = int(params.get("top_k") or params.get("bottom_k") or 5)
    rebalance_interval = int(params.get("rebalance_interval", hold_bars))
    min_universe_size = params.get("min_universe_size")
    min_universe_size = int(min_universe_size) if min_universe_size is not None else None

    def metric_builder(signal_index: int) -> pd.DataFrame:
        ranking_close = close_wide.iloc[signal_index]
        lookback_close = close_wide.iloc[signal_index - lookback_bars]
        momentum = ((ranking_close / lookback_close) - 1.0) * 100.0
        frame = pd.DataFrame({"metric": momentum}).replace([pd.NA, pd.NaT, float("inf"), float("-inf")], pd.NA).dropna()
        return frame

    def selector(metrics: pd.DataFrame) -> pd.DataFrame:
        if strategy_key == "short-term-reversal-basket":
            min_drop_pct = float(params.get("min_drop_pct", 0.0))
            metrics = metrics[metrics["metric"] >= min_drop_pct].copy()
            return metrics.sort_values("metric", ascending=False)
        return metrics.sort_values("metric", ascending=True)

    return _build_fixed_horizon_trades(
        strategy_key=strategy_key,
        close_wide=close_wide,
        open_wide=open_wide,
        lookback_bars=lookback_bars,
        hold_bars=hold_bars,
        rebalance_interval=rebalance_interval,
        top_k=top_k,
        min_universe_size=min_universe_size,
        metric_builder=metric_builder,
        selector=selector,
    )


def _momentum_volatility_inverse(strategy_key: str, market_frames: dict[str, pd.DataFrame], params: dict[str, Any], *, time_column: str) -> list[InverseShortSignal]:
    prepared, close_wide, open_wide = _wide_frames(market_frames, time_column=time_column)
    if close_wide.empty:
        return []
    returns_wide = close_wide.pct_change() * 100.0
    lookback_bars = int(params.get("lookback_bars", 60))
    volatility_window = int(params.get("volatility_window", 60))
    hold_bars = int(params.get("hold_bars", 5))
    top_k = int(params.get("top_k", 5))
    rebalance_interval = int(params.get("rebalance_interval", hold_bars))
    min_volatility_pct = float(params.get("min_volatility_pct", 0.5))
    min_momentum_pct = float(params.get("min_momentum_pct", 0.0))
    min_universe_size = params.get("min_universe_size")
    min_universe_size = int(min_universe_size) if min_universe_size is not None else None
    start_index = max(lookback_bars, volatility_window)

    liquidity_wide = None
    if strategy_key == "liquidity-screened-absolute-momentum-composite":
        liquidity_parts = []
        for symbol, frame in prepared.items():
            if "volume" not in frame.columns:
                continue
            dollar_volume = (frame["close"] * frame["volume"]).rename(symbol)
            liquidity_parts.append(dollar_volume.to_frame().set_index(frame[time_column])[symbol])
        liquidity_wide = pd.concat(liquidity_parts, axis=1).sort_index() if liquidity_parts else None

    breadth_ema_weak_ratio_series = None
    if strategy_key == "breadth-ema-scaled-absolute-momentum-composite":
        breadth_floor = float(params.get("breadth_momentum_floor_pct", 0.0))
        breadth_ema_span = int(params.get("breadth_ema_span", 3))
        all_momentum = ((close_wide / close_wide.shift(lookback_bars)) - 1.0) * 100.0
        realized_volatility_wide = returns_wide.rolling(volatility_window).std()
        valid_mask = all_momentum.notna() & realized_volatility_wide.notna()
        breadth_ema_weak_ratio_series = (
            all_momentum.le(-1.0 * breadth_floor).where(valid_mask).mean(axis=1, skipna=True)
            .ewm(span=breadth_ema_span, adjust=False, min_periods=1)
            .mean()
        )

    def base_metrics(signal_index: int) -> pd.DataFrame:
        ranking_close = close_wide.iloc[signal_index]
        lookback_close = close_wide.iloc[signal_index - lookback_bars]
        momentum = ((ranking_close / lookback_close) - 1.0) * 100.0
        realized_volatility = returns_wide.rolling(volatility_window).std().iloc[signal_index]
        frame = pd.DataFrame({"metric": momentum, "momentum_pct": momentum, "volatility_pct": realized_volatility})
        return frame.replace([pd.NA, pd.NaT, float("inf"), float("-inf")], pd.NA).dropna()

    def metric_builder(signal_index: int) -> pd.DataFrame:
        frame = base_metrics(signal_index)
        if frame.empty:
            return frame
        if strategy_key in {
            "absolute-momentum-volatility-composite",
            "breadth-regime-gated-composite",
            "breadth-scaled-absolute-momentum-composite",
            "breadth-ema-scaled-absolute-momentum-composite",
            "liquidity-screened-absolute-momentum-composite",
        }:
            frame = frame[frame["momentum_pct"] <= (-1.0 * min_momentum_pct)].copy()
        if strategy_key == "breadth-regime-gated-composite":
            floor = float(params.get("breadth_momentum_floor_pct", 0.0))
            min_ratio = float(params.get("min_breadth_ratio", 0.0))
            weak_ratio = float((base_metrics(signal_index)["momentum_pct"] <= (-1.0 * floor)).mean())
            if weak_ratio < min_ratio:
                return pd.DataFrame()
        if strategy_key == "liquidity-screened-absolute-momentum-composite" and liquidity_wide is not None:
            window = int(params.get("liquidity_window", 20))
            ratio = float(params.get("liquidity_universe_ratio", 1.0))
            median_liquidity = liquidity_wide.rolling(window).median().iloc[signal_index].dropna()
            if median_liquidity.empty:
                return pd.DataFrame()
            eligible_count = max(int(len(median_liquidity) * ratio), top_k)
            eligible = set(median_liquidity.sort_values(ascending=False).head(eligible_count).index)
            frame = frame[frame.index.isin(eligible)].copy()
        if frame.empty:
            return frame
        frame["volatility_pct"] = frame["volatility_pct"].clip(lower=min_volatility_pct)
        frame["score"] = frame["momentum_pct"] / frame["volatility_pct"]
        if strategy_key == "breadth-ema-scaled-absolute-momentum-composite" and breadth_ema_weak_ratio_series is not None:
            scale_floor = float(params.get("breadth_scale_floor_ratio", 0.0))
            weak_ratio = breadth_ema_weak_ratio_series.iloc[signal_index]
            if pd.isna(weak_ratio):
                return pd.DataFrame()
            scale = 0.0 if weak_ratio <= scale_floor or scale_floor >= 1.0 else min(max((float(weak_ratio) - scale_floor) / (1.0 - scale_floor), 0.0), 1.0)
            frame.attrs["dynamic_top_k"] = min(len(frame), top_k, max(1, int(top_k * scale))) if scale > 0 else 0
        return frame

    def selector(metrics: pd.DataFrame) -> pd.DataFrame:
        if strategy_key == "breadth-scaled-absolute-momentum-composite":
            all_metrics = metrics
            floor = float(params.get("breadth_momentum_floor_pct", 0.0))
            scale_floor = float(params.get("breadth_scale_floor_ratio", 0.0))
            weak_ratio = float((all_metrics["momentum_pct"] <= (-1.0 * floor)).mean()) if not all_metrics.empty else 0.0
            scale = 0.0 if weak_ratio <= scale_floor or scale_floor >= 1.0 else min(max((weak_ratio - scale_floor) / (1.0 - scale_floor), 0.0), 1.0)
            dynamic_top_k = min(len(metrics), top_k, max(1, int(top_k * scale))) if scale > 0 else 0
            return metrics.sort_values(["score", "momentum_pct"], ascending=True).head(dynamic_top_k)
        if strategy_key == "breadth-ema-scaled-absolute-momentum-composite":
            dynamic_top_k = int(metrics.attrs.get("dynamic_top_k", 0))
            return metrics.sort_values(["score", "momentum_pct"], ascending=True).head(dynamic_top_k)
        return metrics.sort_values(["score", "momentum_pct"], ascending=True)

    return _build_fixed_horizon_trades(
        strategy_key=strategy_key,
        close_wide=close_wide,
        open_wide=open_wide,
        lookback_bars=lookback_bars,
        hold_bars=hold_bars,
        rebalance_interval=rebalance_interval,
        top_k=top_k,
        min_universe_size=min_universe_size,
        metric_builder=metric_builder,
        selector=selector,
        start_index=start_index,
    )


def _single_symbol_channel_inverse(strategy_key: str, symbol: str, frame: pd.DataFrame, params: dict[str, Any], *, time_column: str) -> list[InverseShortSignal]:
    frame = _prepare_frame(frame, time_column=time_column)
    if frame.empty:
        return []
    if strategy_key == "donchian-breakout":
        breakout_window = int(params.get("breakout_window", 20))
        exit_window = int(params.get("exit_window", 10))
        breakout_low = frame["low"].rolling(breakout_window, min_periods=breakout_window).min().shift(1)
        exit_high = frame["high"].rolling(exit_window, min_periods=exit_window).max().shift(1)
        start_index = breakout_window
        max_hold_bars = 0
        squeeze_ready = pd.Series(True, index=frame.index)
    elif strategy_key == "volatility-compression-breakout":
        squeeze_window = int(params.get("squeeze_window", 20))
        breakout_window = int(params.get("breakout_window", 20))
        exit_window = int(params.get("exit_window", 10))
        max_hold_bars = int(params.get("max_hold_bars", 20))
        squeeze_quantile = float(params.get("squeeze_quantile", 0.2))
        rolling_mean = frame["close"].rolling(squeeze_window, min_periods=squeeze_window).mean()
        rolling_std = frame["close"].rolling(squeeze_window, min_periods=squeeze_window).std(ddof=0)
        band_width_pct = ((rolling_std * 4.0) / rolling_mean.replace(0, pd.NA)) * 100.0
        threshold = band_width_pct.rolling(squeeze_window, min_periods=squeeze_window).quantile(squeeze_quantile).shift(1)
        squeeze_ready = band_width_pct.shift(1) <= threshold
        breakout_low = frame["low"].rolling(breakout_window, min_periods=breakout_window).min().shift(1)
        exit_high = frame["high"].rolling(exit_window, min_periods=exit_window).max().shift(1)
        start_index = max((squeeze_window * 2) - 1, breakout_window)
    else:
        raise ValueError(f"unsupported channel inverse strategy: {strategy_key}")

    trades: list[InverseShortSignal] = []
    index = start_index
    last_entry_index = -1
    while index < len(frame) - 1:
        signal_row = frame.iloc[index]
        signal_breakout = breakout_low.iloc[index]
        if pd.isna(signal_breakout) or not bool(squeeze_ready.iloc[index]) or float(signal_row["close"]) >= float(signal_breakout):
            index += 1
            continue
        entry_index = index + 1
        if entry_index <= last_entry_index:
            index += 1
            continue
        exit_index = None
        exit_reason = None
        scan_index = max(entry_index, exit_window)
        while scan_index < len(frame) - 1:
            channel_high = exit_high.iloc[scan_index]
            held_bars = scan_index - entry_index + 1
            if not pd.isna(channel_high) and float(frame.iloc[scan_index]["close"]) > float(channel_high):
                exit_index = scan_index + 1
                exit_reason = "channel_exit"
                break
            if max_hold_bars and held_bars >= max_hold_bars:
                exit_index = scan_index + 1
                exit_reason = "time_stop"
                break
            scan_index += 1
        entry_row = frame.iloc[entry_index]
        exit_row = frame.iloc[exit_index] if exit_index is not None else None
        if exit_index is not None:
            last_entry_index = exit_index
        else:
            last_entry_index = len(frame) - 1
        trades.append(
            _fixed_horizon_signal(
                strategy_key=strategy_key,
                symbol=symbol,
                signal_time=signal_row[time_column],
                lookback_time=frame.iloc[max(index - breakout_window + 1, 0)][time_column],
                entry_time=entry_row[time_column],
                entry_price=float(entry_row["open"]),
                exit_time=exit_row[time_column] if exit_row is not None else None,
                exit_price=float(exit_row["open"]) if exit_row is not None else None,
                metric=float(signal_row["close"] - float(signal_breakout)),
                rank=1,
                candidate_count=1,
                planned_hold_bars=(exit_index - entry_index) if exit_index is not None else 0,
                p0_price=float(signal_breakout),
                p1_price=float(signal_row["close"]),
                p0_index=max(index - breakout_window + 1, 0),
                p5_index=index,
                entry_index=entry_index,
                exit_index=exit_index,
            )
        )
        if trades[-1].exit_reason != exit_reason and exit_reason is not None:
            object.__setattr__(trades[-1], "exit_reason", exit_reason)
        index = (exit_index + 1) if exit_index is not None else index + 1
    return trades


def _ema_inverse(symbol: str, frame: pd.DataFrame, params: dict[str, Any], *, time_column: str) -> list[InverseShortSignal]:
    frame = _prepare_frame(frame, time_column=time_column)
    fast_window = int(params.get("fast_window", 20))
    slow_window = int(params.get("slow_window", 50))
    slope_window = int(params.get("slope_window", 5))
    if len(frame) < max(slow_window + slope_window + 2, slow_window + 3):
        return []
    close = frame["close"]
    ema_fast = close.ewm(span=fast_window, adjust=False).mean()
    ema_slow = close.ewm(span=slow_window, adjust=False).mean()
    slow_slope_down = ema_slow < ema_slow.shift(slope_window)
    short_state = (close < ema_slow) & (ema_fast < ema_slow) & slow_slope_down
    entry_signal = short_state & (~short_state.shift(1, fill_value=False))
    exit_signal = (close > ema_slow) | (ema_fast > ema_slow)
    return _event_signals(
        strategy_key="ema-trend-following",
        symbol=symbol,
        frame=frame,
        entry_signal=entry_signal,
        exit_signal=exit_signal,
        start_index=max(slow_window, slope_window),
        time_column=time_column,
        metric_series=((ema_fast / ema_slow) - 1.0) * 100.0,
        exit_reason="trend_break",
    )


def _zscore_inverse(symbol: str, frame: pd.DataFrame, params: dict[str, Any], *, time_column: str) -> list[InverseShortSignal]:
    frame = _prepare_frame(frame, time_column=time_column)
    lookback = int(params.get("lookback", 20))
    entry_z = float(params.get("entry_z", 2.0))
    exit_z = float(params.get("exit_z", 0.5))
    max_hold_bars = int(params.get("max_hold_bars", 10))
    if len(frame) < lookback + 2:
        return []
    close = frame["close"]
    rolling_mean = close.rolling(lookback, min_periods=lookback).mean()
    rolling_std = close.rolling(lookback, min_periods=lookback).std(ddof=0).mask(lambda values: values == 0.0)
    zscore = ((close - rolling_mean) / rolling_std).astype("float64")
    prior_z = zscore.shift(1)
    entry_signal = (zscore >= entry_z) & ((prior_z < entry_z) | prior_z.isna())
    return _event_signals(
        strategy_key="zscore-mean-reversion",
        symbol=symbol,
        frame=frame,
        entry_signal=entry_signal,
        exit_signal=zscore <= exit_z,
        start_index=lookback,
        time_column=time_column,
        metric_series=zscore,
        exit_reason="mean_reversion_exit",
        max_hold_bars=max_hold_bars,
    )


def _event_signals(
    *,
    strategy_key: str,
    symbol: str,
    frame: pd.DataFrame,
    entry_signal: pd.Series,
    exit_signal: pd.Series,
    start_index: int,
    time_column: str,
    metric_series: pd.Series,
    exit_reason: str,
    max_hold_bars: int | None = None,
) -> list[InverseShortSignal]:
    trades: list[InverseShortSignal] = []
    index = start_index
    last_entry_index = -1
    while index < len(frame) - 1:
        if not bool(entry_signal.iloc[index]):
            index += 1
            continue
        entry_index = index + 1
        if entry_index <= last_entry_index:
            index += 1
            continue
        exit_index = None
        resolved_exit_reason = exit_reason
        scan_index = entry_index
        while scan_index < len(frame) - 1:
            if bool(exit_signal.iloc[scan_index]):
                exit_index = scan_index + 1
                break
            if max_hold_bars is not None and scan_index - entry_index >= max_hold_bars:
                exit_index = scan_index + 1
                resolved_exit_reason = "time_stop"
                break
            scan_index += 1
        entry_row = frame.iloc[entry_index]
        exit_row = frame.iloc[exit_index] if exit_index is not None else None
        if exit_index is not None:
            last_entry_index = exit_index
        else:
            last_entry_index = len(frame) - 1
        trade = _fixed_horizon_signal(
            strategy_key=strategy_key,
            symbol=symbol,
            signal_time=frame.iloc[index][time_column],
            lookback_time=frame.iloc[max(index - start_index, 0)][time_column],
            entry_time=entry_row[time_column],
            entry_price=float(entry_row["open"]),
            exit_time=exit_row[time_column] if exit_row is not None else None,
            exit_price=float(exit_row["open"]) if exit_row is not None else None,
            metric=float(metric_series.iloc[index]) if not pd.isna(metric_series.iloc[index]) else 0.0,
            rank=1,
            candidate_count=1,
            planned_hold_bars=(exit_index - entry_index) if exit_index is not None else 0,
            p0_price=float(metric_series.iloc[index]) if not pd.isna(metric_series.iloc[index]) else 0.0,
            p1_price=float(frame.iloc[index]["close"]),
            p0_index=max(index - start_index, 0),
            p5_index=index,
            entry_index=entry_index,
            exit_index=exit_index,
        )
        if exit_index is not None:
            object.__setattr__(trade, "exit_reason", resolved_exit_reason)
        trades.append(trade)
        index = (exit_index + 1) if exit_index is not None else index + 1
    return trades


def _five_wave_inverse(symbol: str, frame: pd.DataFrame, params: dict[str, Any], *, time_column: str, exit_mode: str | None) -> list[InverseShortSignal]:
    frame = _prepare_frame(frame, time_column=time_column)
    reversal_pct = float(params.get("reversal_pct", REVERSAL_PCT))
    trailing_stop_pct = float(params.get("trailing_stop_pct") or DEFAULT_TRAILING_STOP_PCT)
    mode = exit_mode or EXIT_MODE_THREE_WAVE
    pivots = build_zigzag_pivots(frame, reversal_pct=reversal_pct, time_column=time_column)
    trades: list[InverseShortSignal] = []
    skip_until_index = -1
    for start in range(0, max(len(pivots) - 5, 0)):
        window = pivots[start : start + 6]
        trade = _build_five_wave_short_trade(
            frame,
            symbol,
            window,
            reversal_pct=reversal_pct,
            time_column=time_column,
            exit_mode=mode,
            trailing_stop_pct=trailing_stop_pct,
        )
        if trade is None:
            continue
        if trade.entry_index <= skip_until_index:
            continue
        trades.append(trade)
        skip_until_index = trade.exit_index if trade.exit_index is not None else len(frame) - 1
    return trades


def _build_five_wave_short_trade(
    frame: pd.DataFrame,
    symbol: str,
    pivots: list[Pivot],
    *,
    reversal_pct: float,
    time_column: str,
    exit_mode: str,
    trailing_stop_pct: float,
) -> InverseShortSignal | None:
    if len(pivots) != 6 or [pivot.kind for pivot in pivots] != ["low", "high", "low", "high", "low", "high"]:
        return None
    p0, p1, p2, p3, p4, p5 = pivots
    if not all(_bars_in_wave(left, right) >= MIN_WAVE_BAR_COUNT for left, right in ((p0, p1), (p1, p2), (p2, p3), (p3, p4), (p4, p5))):
        return None
    if not (p0.price < p2.price < p4.price) or not (p1.price < p3.price < p5.price):
        return None
    wave_rise_pct = ((p5.price / p0.price) - 1.0) * 100.0 if p0.price else 0.0
    if wave_rise_pct < abs(MIN_WAVE_DROP_PCT):
        return None
    speed1 = (p1.price - p0.price) / _bars_between(p0, p1)
    speed3 = (p3.price - p2.price) / _bars_between(p2, p3)
    speed5 = (p5.price - p4.price) / _bars_between(p4, p5)
    if not (speed5 > speed1 and speed5 > speed3):
        return None
    fractal = _find_top_fractal(frame, p5.index)
    if fractal is None:
        return None
    fractal_center_index, confirm_index, entry_index = fractal
    hold_bars = max(_bars_in_wave(p0, p5), 1)
    planned_exit_index = entry_index + hold_bars
    entry_row = frame.iloc[entry_index]
    entry_price = float(entry_row["open"])
    exit_date, exit_price, holding_bars, exit_reason, exit_index, status = _resolve_five_wave_short_exit(
        frame,
        entry_index=entry_index,
        planned_exit_index=planned_exit_index,
        entry_price=entry_price,
        reversal_pct=reversal_pct,
        time_column=time_column,
        exit_mode=exit_mode,
        trailing_stop_pct=trailing_stop_pct,
    )
    return_pct = ((exit_price / entry_price) - 1.0) * 100.0 if exit_price is not None else None
    return InverseShortSignal(
        signal_id=f"{symbol}-{_format_signal_time(p0.trade_date)}-{_format_signal_time(entry_row[time_column])}-five-wave-inverse",
        symbol=symbol,
        wave_start_date=p0.trade_date,
        wave_end_date=p5.trade_date,
        p0_price=round(p0.price, 6),
        p1_price=round(p1.price, 6),
        p2_price=round(p2.price, 6),
        p3_price=round(p3.price, 6),
        p4_price=round(p4.price, 6),
        p5_price=round(p5.price, 6),
        p0_date=p0.trade_date,
        p1_date=p1.trade_date,
        p2_date=p2.trade_date,
        p3_date=p3.trade_date,
        p4_date=p4.trade_date,
        p5_date=p5.trade_date,
        wave_drop_pct=round(wave_rise_pct, 4),
        speed1=round(speed1, 6),
        speed3=round(speed3, 6),
        speed5=round(speed5, 6),
        fractal_center_date=frame.iloc[fractal_center_index][time_column],
        signal_confirm_date=frame.iloc[confirm_index][time_column],
        entry_date=entry_row[time_column],
        entry_price=round(entry_price, 6),
        planned_hold_bars=hold_bars,
        exit_date=exit_date,
        exit_price=round(exit_price, 6) if exit_price is not None else None,
        return_pct=round(return_pct, 4) if return_pct is not None else None,
        holding_days=holding_bars,
        status=status,
        exit_reason=exit_reason,
        p0_index=p0.index,
        p5_index=p5.index,
        entry_index=entry_index,
        exit_index=exit_index,
    )


def _resolve_five_wave_short_exit(
    frame: pd.DataFrame,
    *,
    entry_index: int,
    planned_exit_index: int,
    entry_price: float,
    reversal_pct: float,
    time_column: str,
    exit_mode: str,
    trailing_stop_pct: float,
) -> tuple[Any | None, float | None, int | None, str | None, int | None, str]:
    max_exit_index = min(planned_exit_index, len(frame) - 1)
    if exit_mode == EXIT_MODE_TRAILING_STOP:
        trailing_trough = entry_price
        for index in range(entry_index, min(planned_exit_index, len(frame))):
            row = frame.iloc[index]
            stop_price = trailing_trough * (1.0 + trailing_stop_pct)
            if float(row["open"]) >= stop_price:
                return row[time_column], float(row["open"]), index - entry_index, "stop_loss", index, "closed"
            if float(row["high"]) > stop_price:
                return row[time_column], stop_price, index - entry_index, "stop_loss", index, "closed"
            trailing_trough = min(trailing_trough, float(row["low"]))
    elif exit_mode == EXIT_MODE_THREE_WAVE:
        pivots = _map_pivots_to_global(frame, entry_index=entry_index, reversal_pct=reversal_pct, time_column=time_column)
        for start in range(0, max(len(pivots) - 5, 0)):
            window = pivots[start : start + 6]
            if len(window) < 6 or [pivot.kind for pivot in window] != ["high", "low", "high", "low", "high", "low"]:
                continue
            p0, p1, p2, p3, p4, p5 = window
            if p0.index < entry_index:
                continue
            if not (p0.price > p2.price > p4.price) or not (p1.price > p3.price > p5.price):
                continue
            fractal = _find_bottom_fractal(frame, p5.index)
            if fractal is None:
                continue
            _, _, exit_index = fractal
            if exit_index > max_exit_index:
                continue
            row = frame.iloc[exit_index]
            return row[time_column], float(row["open"]), exit_index - entry_index, "pattern_exit", exit_index, "closed"
    elif exit_mode != EXIT_MODE_TIME_ONLY:
        raise ValueError(f"unsupported five-wave exit mode: {exit_mode}")
    if planned_exit_index < len(frame):
        row = frame.iloc[planned_exit_index]
        return row[time_column], float(row["open"]), planned_exit_index - entry_index, "time_exit", planned_exit_index, "closed"
    return None, None, None, None, None, "incomplete"


def build_inverse_short_signals(
    *,
    strategy_key: str,
    market_frames: dict[str, pd.DataFrame],
    params: dict[str, Any],
    timeframe: str,
    exit_mode: str | None = None,
    time_column: str = "bar_time",
) -> list[InverseShortSignal]:
    del timeframe
    if strategy_key in {"cross-sectional-relative-strength", "short-term-reversal-basket"}:
        return _cross_sectional_inverse(strategy_key, market_frames, params, time_column=time_column)
    if strategy_key in {
        "momentum-volatility-composite",
        "absolute-momentum-volatility-composite",
        "breadth-regime-gated-composite",
        "breadth-scaled-absolute-momentum-composite",
        "breadth-ema-scaled-absolute-momentum-composite",
        "liquidity-screened-absolute-momentum-composite",
    }:
        return _momentum_volatility_inverse(strategy_key, market_frames, params, time_column=time_column)

    trades: list[InverseShortSignal] = []
    for symbol, frame in market_frames.items():
        if strategy_key in {"donchian-breakout", "volatility-compression-breakout"}:
            trades.extend(_single_symbol_channel_inverse(strategy_key, symbol, frame, params, time_column=time_column))
        elif strategy_key == "ema-trend-following":
            trades.extend(_ema_inverse(symbol, frame, params, time_column=time_column))
        elif strategy_key == "zscore-mean-reversion":
            trades.extend(_zscore_inverse(symbol, frame, params, time_column=time_column))
        elif strategy_key == "five-wave-reversal":
            trades.extend(_five_wave_inverse(symbol, frame, params, time_column=time_column, exit_mode=exit_mode))
        else:
            raise ValueError(f"unsupported strategy_key for inverse short: {strategy_key}")
    return trades


__all__ = ["INVERSE_DEFINITION", "InverseShortSignal", "build_inverse_short_signals"]
