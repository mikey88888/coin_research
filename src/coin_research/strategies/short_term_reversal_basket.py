from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd


DEFAULT_LOOKBACK_BARS = 3
DEFAULT_HOLD_BARS = 3
DEFAULT_BOTTOM_K = 5
DEFAULT_REBALANCE_INTERVAL = 3
DEFAULT_MIN_DROP_PCT = 0.0
EXIT_REASON_REBALANCE = "rebalance_exit"


@dataclass(frozen=True)
class ShortTermReversalBasketTrade:
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
class ShortTermReversalBasketResult:
    trades: list[ShortTermReversalBasketTrade]


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
    frame = frame[(frame["open"] > 0) & (frame["high"] > 0) & (frame["low"] > 0) & (frame["close"] > 0)]
    frame = frame.dropna(subset=[time_column, "open", "high", "low", "close"]).sort_values(time_column).reset_index(drop=True)
    return frame


def _validate_params(
    *,
    lookback_bars: int,
    hold_bars: int,
    bottom_k: int,
    rebalance_interval: int,
    min_universe_size: int | None,
    min_drop_pct: float,
) -> int:
    if lookback_bars <= 0:
        raise ValueError(f"lookback_bars must be > 0, got {lookback_bars}")
    if hold_bars <= 0:
        raise ValueError(f"hold_bars must be > 0, got {hold_bars}")
    if bottom_k <= 0:
        raise ValueError(f"bottom_k must be > 0, got {bottom_k}")
    if rebalance_interval <= 0:
        raise ValueError(f"rebalance_interval must be > 0, got {rebalance_interval}")
    if min_drop_pct < 0:
        raise ValueError(f"min_drop_pct must be >= 0, got {min_drop_pct}")
    required_universe = min_universe_size if min_universe_size is not None else bottom_k
    if required_universe < bottom_k:
        raise ValueError(f"min_universe_size must be >= bottom_k, got min_universe_size={required_universe}, bottom_k={bottom_k}")
    return required_universe


def summarize_trade_results(
    trades: list[ShortTermReversalBasketTrade],
    *,
    universe_symbols: int,
    rebalance_interval: int,
    bottom_k: int,
    min_drop_pct: float,
) -> dict[str, float | int | None]:
    closed = [trade for trade in trades if trade.status == "closed" and trade.return_pct is not None]
    returns = pd.Series([float(trade.return_pct) for trade in closed], dtype="float64")
    holding_bars = pd.Series([float(trade.holding_days or 0) for trade in closed], dtype="float64")
    selected_lookback_returns = pd.Series([float(trade.wave_drop_pct) for trade in trades], dtype="float64") if trades else pd.Series(dtype="float64")
    rebalance_count = len({trade.signal_confirm_date for trade in trades})
    return {
        "universe_symbols": universe_symbols,
        "rebalance_interval": rebalance_interval,
        "bottom_k": bottom_k,
        "min_drop_pct": min_drop_pct,
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
        "avg_selected_lookback_return_pct": round(float(selected_lookback_returns.mean()), 4) if not selected_lookback_returns.empty else None,
        "avg_selected_drop_magnitude_pct": round(float(selected_lookback_returns.abs().mean()), 4) if not selected_lookback_returns.empty else None,
        "rebalance_exit_count": sum(1 for trade in closed if trade.exit_reason == EXIT_REASON_REBALANCE),
    }


def run_short_term_reversal_basket_backtest(
    market_frames: dict[str, pd.DataFrame],
    *,
    lookback_bars: int = DEFAULT_LOOKBACK_BARS,
    hold_bars: int = DEFAULT_HOLD_BARS,
    bottom_k: int = DEFAULT_BOTTOM_K,
    rebalance_interval: int = DEFAULT_REBALANCE_INTERVAL,
    min_universe_size: int | None = None,
    min_drop_pct: float = DEFAULT_MIN_DROP_PCT,
    time_column: str = "bar_time",
    enforce_non_overlapping: bool = True,
) -> ShortTermReversalBasketResult:
    required_universe = _validate_params(
        lookback_bars=lookback_bars,
        hold_bars=hold_bars,
        bottom_k=bottom_k,
        rebalance_interval=rebalance_interval,
        min_universe_size=min_universe_size,
        min_drop_pct=min_drop_pct,
    )

    prepared_frames: dict[str, pd.DataFrame] = {}
    for symbol, frame in market_frames.items():
        prepared = _prepare_frame(frame, time_column=time_column)
        if prepared.empty:
            continue
        prepared_frames[symbol] = prepared
    if not prepared_frames:
        return ShortTermReversalBasketResult(trades=[])

    close_wide = pd.concat(
        [frame.set_index(time_column)["close"].rename(symbol) for symbol, frame in prepared_frames.items()],
        axis=1,
    ).sort_index()
    open_wide = pd.concat(
        [frame.set_index(time_column)["open"].rename(symbol) for symbol, frame in prepared_frames.items()],
        axis=1,
    ).sort_index()
    timeline = list(close_wide.index)
    if len(timeline) <= lookback_bars + 1:
        return ShortTermReversalBasketResult(trades=[])

    trades: list[ShortTermReversalBasketTrade] = []
    next_available_entry_idx: dict[str, int] = {}

    for signal_index in range(lookback_bars, len(timeline) - 1, rebalance_interval):
        signal_time = timeline[signal_index]
        lookback_time = timeline[signal_index - lookback_bars]
        entry_index = signal_index + 1
        if entry_index >= len(timeline):
            break

        ranking_close = close_wide.iloc[signal_index]
        lookback_close = close_wide.iloc[signal_index - lookback_bars]
        lookback_returns = ((ranking_close / lookback_close) - 1.0) * 100.0
        ranking_frame = pd.DataFrame({"lookback_return_pct": lookback_returns})
        ranking_frame = ranking_frame.replace([pd.NA, pd.NaT, float("inf"), float("-inf")], pd.NA).dropna()
        ranking_frame = ranking_frame[ranking_frame["lookback_return_pct"] <= (-1.0 * min_drop_pct)]
        if len(ranking_frame) < required_universe:
            continue

        selected = ranking_frame.sort_values(["lookback_return_pct"], ascending=True).head(bottom_k)
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

            lookback_return_pct = float(row["lookback_return_pct"])
            trades.append(
                ShortTermReversalBasketTrade(
                    signal_id=(
                        f"{symbol}-{_format_signal_time(signal_time)}-"
                        f"lb{lookback_bars}-bot{bottom_k}-h{hold_bars}-r{rank}"
                    ),
                    symbol=symbol,
                    wave_start_date=lookback_time,
                    wave_end_date=signal_time,
                    p0_price=round(float(lookback_close[symbol]), 6),
                    p1_price=round(float(ranking_close[symbol]), 6),
                    p2_price=round(float(entry_price), 6),
                    p3_price=round(float(exit_price), 6) if exit_price is not None else 0.0,
                    p4_price=float(rank),
                    p5_price=float(len(ranking_frame)),
                    p0_date=lookback_time,
                    p1_date=signal_time,
                    p2_date=timeline[entry_index],
                    p3_date=exit_time,
                    p4_date=None,
                    p5_date=None,
                    wave_drop_pct=round(lookback_return_pct, 4),
                    speed1=float(rank),
                    speed3=float(len(ranking_frame)),
                    speed5=round(abs(lookback_return_pct), 6),
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

    return ShortTermReversalBasketResult(trades=trades)


__all__ = [
    "DEFAULT_BOTTOM_K",
    "DEFAULT_HOLD_BARS",
    "DEFAULT_LOOKBACK_BARS",
    "DEFAULT_MIN_DROP_PCT",
    "DEFAULT_REBALANCE_INTERVAL",
    "EXIT_REASON_REBALANCE",
    "ShortTermReversalBasketResult",
    "ShortTermReversalBasketTrade",
    "run_short_term_reversal_basket_backtest",
    "summarize_trade_results",
]
