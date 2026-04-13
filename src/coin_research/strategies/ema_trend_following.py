from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd


DEFAULT_FAST_WINDOW = 20
DEFAULT_SLOW_WINDOW = 50
DEFAULT_SLOPE_WINDOW = 5
EXIT_REASON_TREND_BREAK = "trend_break"


@dataclass(frozen=True)
class EmaTrendTrade:
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
class EmaTrendTradeResult:
    trades: list[EmaTrendTrade]


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


def _validate_windows(*, fast_window: int, slow_window: int, slope_window: int) -> None:
    if fast_window <= 0:
        raise ValueError(f"fast_window must be > 0, got {fast_window}")
    if slow_window <= fast_window:
        raise ValueError(f"slow_window must be greater than fast_window, got fast_window={fast_window}, slow_window={slow_window}")
    if slope_window <= 0:
        raise ValueError(f"slope_window must be > 0, got {slope_window}")


def summarize_trade_results(trades: list[EmaTrendTrade], *, universe_symbols: int) -> dict[str, float | int | None]:
    closed = [trade for trade in trades if trade.status == "closed" and trade.return_pct is not None]
    returns = pd.Series([float(trade.return_pct) for trade in closed], dtype="float64")
    holding_bars = pd.Series([float(trade.holding_days or 0) for trade in closed], dtype="float64")
    return {
        "universe_symbols": universe_symbols,
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
        "trend_break_exit_count": sum(1 for trade in closed if trade.exit_reason == EXIT_REASON_TREND_BREAK),
    }


def run_ema_trend_following_backtest(
    df: pd.DataFrame,
    *,
    symbol: str,
    fast_window: int = DEFAULT_FAST_WINDOW,
    slow_window: int = DEFAULT_SLOW_WINDOW,
    slope_window: int = DEFAULT_SLOPE_WINDOW,
    time_column: str = "bar_time",
    enforce_non_overlapping: bool = True,
) -> EmaTrendTradeResult:
    _validate_windows(fast_window=fast_window, slow_window=slow_window, slope_window=slope_window)
    frame = _prepare_frame(df, time_column=time_column)
    minimum_length = max(slow_window + slope_window + 2, slow_window + 3)
    if len(frame) < minimum_length:
        return EmaTrendTradeResult(trades=[])

    close = frame["close"]
    ema_fast = close.ewm(span=fast_window, adjust=False).mean()
    ema_slow = close.ewm(span=slow_window, adjust=False).mean()
    slow_slope_up = ema_slow > ema_slow.shift(slope_window)
    trend_state = (close > ema_slow) & (ema_fast > ema_slow) & slow_slope_up
    entry_signal = trend_state & (~trend_state.shift(1, fill_value=False))
    exit_signal = (close < ema_slow) | (ema_fast < ema_slow)

    trades: list[EmaTrendTrade] = []
    index = max(slow_window, slope_window)
    last_entry_index = -1

    while index < len(frame) - 1:
        if not bool(entry_signal.iloc[index]):
            index += 1
            continue

        entry_index = index + 1
        if entry_index >= len(frame):
            break
        if enforce_non_overlapping and entry_index <= last_entry_index:
            index += 1
            continue

        signal_row = frame.iloc[index]
        entry_row = frame.iloc[entry_index]
        entry_price = float(entry_row["open"])
        exit_index = None
        exit_reason = None

        scan_index = entry_index
        while scan_index < len(frame) - 1:
            if bool(exit_signal.iloc[scan_index]):
                exit_index = scan_index + 1
                exit_reason = EXIT_REASON_TREND_BREAK
                break
            scan_index += 1

        if exit_index is None:
            exit_date = None
            exit_price = None
            holding_bars = None
            return_pct = None
            status = "incomplete"
            last_entry_index = len(frame) - 1 if enforce_non_overlapping else last_entry_index
            exit_signal_row = None
        else:
            exit_signal_row = frame.iloc[scan_index]
            exit_row = frame.iloc[exit_index]
            exit_date = exit_row[time_column]
            exit_price = float(exit_row["open"])
            holding_bars = exit_index - entry_index
            return_pct = ((exit_price / entry_price) - 1.0) * 100.0
            status = "closed"
            last_entry_index = exit_index

        lookback_index = max(index - slow_window + 1, 0)
        lookback_row = frame.iloc[lookback_index]
        ema_fast_signal = float(ema_fast.iloc[index])
        ema_slow_signal = float(ema_slow.iloc[index])
        ema_spread_pct = ((ema_fast_signal / ema_slow_signal) - 1.0) * 100.0 if ema_slow_signal else 0.0
        slow_slope_pct = (
            ((ema_slow_signal / float(ema_slow.iloc[index - slope_window])) - 1.0) * 100.0
            if index - slope_window >= 0 and float(ema_slow.iloc[index - slope_window])
            else 0.0
        )
        exit_signal_price = float(ema_slow.iloc[scan_index]) if exit_index is not None else None

        trades.append(
            EmaTrendTrade(
                signal_id=f"{symbol}-{_format_signal_time(signal_row[time_column])}-{fast_window}-{slow_window}-{slope_window}",
                symbol=symbol,
                wave_start_date=lookback_row[time_column],
                wave_end_date=signal_row[time_column],
                p0_price=round(ema_fast_signal, 6),
                p1_price=round(ema_slow_signal, 6),
                p2_price=round(entry_price, 6),
                p3_price=round(exit_signal_price, 6) if exit_signal_price is not None else 0.0,
                p4_price=round(exit_price, 6) if exit_price is not None else 0.0,
                p5_price=0.0,
                p0_date=lookback_row[time_column],
                p1_date=signal_row[time_column],
                p2_date=entry_row[time_column],
                p3_date=exit_signal_row[time_column] if exit_signal_row is not None else None,
                p4_date=exit_date,
                p5_date=None,
                wave_drop_pct=round(ema_spread_pct, 4),
                speed1=round(slow_slope_pct, 4),
                speed3=0.0,
                speed5=0.0,
                fractal_center_date=signal_row[time_column],
                signal_confirm_date=signal_row[time_column],
                entry_date=entry_row[time_column],
                entry_price=round(entry_price, 6),
                planned_hold_bars=0,
                exit_date=exit_date,
                exit_price=round(exit_price, 6) if exit_price is not None else None,
                return_pct=round(return_pct, 4) if return_pct is not None else None,
                holding_days=holding_bars,
                status=status,
                exit_reason=exit_reason,
                p0_index=lookback_index,
                p5_index=index,
                entry_index=entry_index,
                exit_index=exit_index,
            )
        )

        if enforce_non_overlapping and exit_index is not None:
            index = exit_index + 1
        else:
            index += 1

    return EmaTrendTradeResult(trades=trades)


__all__ = [
    "DEFAULT_FAST_WINDOW",
    "DEFAULT_SLOW_WINDOW",
    "DEFAULT_SLOPE_WINDOW",
    "EXIT_REASON_TREND_BREAK",
    "EmaTrendTrade",
    "EmaTrendTradeResult",
    "run_ema_trend_following_backtest",
    "summarize_trade_results",
]
