from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd


DEFAULT_BREAKOUT_WINDOW = 20
DEFAULT_EXIT_WINDOW = 10
EXIT_REASON_CHANNEL = "channel_exit"


@dataclass(frozen=True)
class DonchianTrade:
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
class DonchianTradeResult:
    trades: list[DonchianTrade]


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


def _validate_windows(*, breakout_window: int, exit_window: int) -> None:
    if breakout_window <= 1:
        raise ValueError(f"breakout_window must be > 1, got {breakout_window}")
    if exit_window <= 0:
        raise ValueError(f"exit_window must be > 0, got {exit_window}")
    if exit_window >= breakout_window:
        raise ValueError(
            f"exit_window must be smaller than breakout_window for a Donchian breakout prototype, got breakout_window={breakout_window}, exit_window={exit_window}"
        )


def summarize_trade_results(trades: list[DonchianTrade], *, universe_symbols: int) -> dict[str, float | int | None]:
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
        "channel_exit_count": sum(1 for trade in closed if trade.exit_reason == EXIT_REASON_CHANNEL),
    }


def run_donchian_breakout_backtest(
    df: pd.DataFrame,
    *,
    symbol: str,
    breakout_window: int = DEFAULT_BREAKOUT_WINDOW,
    exit_window: int = DEFAULT_EXIT_WINDOW,
    time_column: str = "bar_time",
    enforce_non_overlapping: bool = True,
) -> DonchianTradeResult:
    _validate_windows(breakout_window=breakout_window, exit_window=exit_window)
    frame = _prepare_frame(df, time_column=time_column)
    if len(frame) < breakout_window + 2:
        return DonchianTradeResult(trades=[])

    breakout_high = frame["high"].rolling(window=breakout_window, min_periods=breakout_window).max().shift(1)
    exit_low = frame["low"].rolling(window=exit_window, min_periods=exit_window).min().shift(1)

    trades: list[DonchianTrade] = []
    index = breakout_window
    last_entry_index = -1

    while index < len(frame) - 1:
        signal_row = frame.iloc[index]
        signal_close = float(signal_row["close"])
        signal_high = breakout_high.iloc[index]
        if pd.isna(signal_high) or signal_close <= float(signal_high):
            index += 1
            continue

        entry_index = index + 1
        if entry_index >= len(frame):
            break
        if enforce_non_overlapping and entry_index <= last_entry_index:
            index += 1
            continue

        entry_row = frame.iloc[entry_index]
        entry_price = float(entry_row["open"])
        exit_index = None
        exit_reason = None

        scan_index = max(entry_index + 1, breakout_window)
        while scan_index < len(frame) - 1:
            channel_low = exit_low.iloc[scan_index]
            if not pd.isna(channel_low) and float(frame.iloc[scan_index]["close"]) < float(channel_low):
                exit_index = scan_index + 1
                exit_reason = EXIT_REASON_CHANNEL
                break
            scan_index += 1

        if exit_index is None:
            exit_date = None
            exit_price = None
            holding_bars = None
            return_pct = None
            status = "incomplete"
            last_entry_index = len(frame) - 1 if enforce_non_overlapping else last_entry_index
        else:
            exit_row = frame.iloc[exit_index]
            exit_date = exit_row[time_column]
            exit_price = float(exit_row["open"])
            holding_bars = exit_index - entry_index
            return_pct = ((exit_price / entry_price) - 1.0) * 100.0
            status = "closed"
            last_entry_index = exit_index

        channel_start_index = max(index - breakout_window + 1, 0)
        channel_start_row = frame.iloc[channel_start_index]
        signal_time = signal_row[time_column]
        trades.append(
            DonchianTrade(
                signal_id=f"{symbol}-{_format_signal_time(signal_time)}-{breakout_window}-{exit_window}",
                symbol=symbol,
                wave_start_date=channel_start_row[time_column],
                wave_end_date=signal_time,
                p0_price=round(float(signal_high), 6),
                p1_price=round(float(signal_row["close"]), 6),
                p2_price=round(entry_price, 6),
                p3_price=round(float(exit_low.iloc[index]) if not pd.isna(exit_low.iloc[index]) else 0.0, 6),
                p4_price=round(exit_price, 6) if exit_price is not None else 0.0,
                p5_price=0.0,
                p0_date=channel_start_row[time_column],
                p1_date=signal_time,
                p2_date=entry_row[time_column],
                p3_date=frame.iloc[index][time_column],
                p4_date=exit_date,
                p5_date=None,
                wave_drop_pct=0.0,
                speed1=0.0,
                speed3=0.0,
                speed5=0.0,
                fractal_center_date=signal_time,
                signal_confirm_date=signal_time,
                entry_date=entry_row[time_column],
                entry_price=round(entry_price, 6),
                planned_hold_bars=0,
                exit_date=exit_date,
                exit_price=round(exit_price, 6) if exit_price is not None else None,
                return_pct=round(return_pct, 4) if return_pct is not None else None,
                holding_days=holding_bars,
                status=status,
                exit_reason=exit_reason,
                p0_index=channel_start_index,
                p5_index=index,
                entry_index=entry_index,
                exit_index=exit_index,
            )
        )

        if enforce_non_overlapping and exit_index is not None:
            index = exit_index + 1
        else:
            index += 1

    return DonchianTradeResult(trades=trades)


__all__ = [
    "DEFAULT_BREAKOUT_WINDOW",
    "DEFAULT_EXIT_WINDOW",
    "EXIT_REASON_CHANNEL",
    "DonchianTrade",
    "DonchianTradeResult",
    "run_donchian_breakout_backtest",
    "summarize_trade_results",
]
