from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd


DEFAULT_LOOKBACK = 20
DEFAULT_ENTRY_Z = 2.0
DEFAULT_EXIT_Z = 0.0
DEFAULT_MAX_HOLD_BARS = 10
EXIT_REASON_MEAN_REVERSION = "mean_reversion"
EXIT_REASON_TIME_STOP = "time_stop"


@dataclass(frozen=True)
class ZScoreMeanReversionTrade:
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
class ZScoreMeanReversionTradeResult:
    trades: list[ZScoreMeanReversionTrade]


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


def _validate_params(*, lookback: int, entry_z: float, exit_z: float, max_hold_bars: int) -> None:
    if lookback <= 1:
        raise ValueError(f"lookback must be > 1, got {lookback}")
    if entry_z <= 0:
        raise ValueError(f"entry_z must be > 0, got {entry_z}")
    if exit_z < 0:
        raise ValueError(f"exit_z must be >= 0, got {exit_z}")
    if entry_z <= exit_z:
        raise ValueError(f"entry_z must be greater than exit_z for an oversold rebound prototype, got entry_z={entry_z}, exit_z={exit_z}")
    if max_hold_bars <= 0:
        raise ValueError(f"max_hold_bars must be > 0, got {max_hold_bars}")


def summarize_trade_results(trades: list[ZScoreMeanReversionTrade], *, universe_symbols: int) -> dict[str, float | int | None]:
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
        "mean_reversion_exit_count": sum(1 for trade in closed if trade.exit_reason == EXIT_REASON_MEAN_REVERSION),
        "time_stop_exit_count": sum(1 for trade in closed if trade.exit_reason == EXIT_REASON_TIME_STOP),
    }


def run_zscore_mean_reversion_backtest(
    df: pd.DataFrame,
    *,
    symbol: str,
    lookback: int = DEFAULT_LOOKBACK,
    entry_z: float = DEFAULT_ENTRY_Z,
    exit_z: float = DEFAULT_EXIT_Z,
    max_hold_bars: int = DEFAULT_MAX_HOLD_BARS,
    time_column: str = "bar_time",
    enforce_non_overlapping: bool = True,
) -> ZScoreMeanReversionTradeResult:
    _validate_params(lookback=lookback, entry_z=entry_z, exit_z=exit_z, max_hold_bars=max_hold_bars)
    frame = _prepare_frame(df, time_column=time_column)
    if len(frame) < lookback + 2:
        return ZScoreMeanReversionTradeResult(trades=[])

    close = frame["close"]
    rolling_mean = close.rolling(window=lookback, min_periods=lookback).mean()
    rolling_std = close.rolling(window=lookback, min_periods=lookback).std(ddof=0).mask(lambda values: values == 0.0)
    zscore = ((close - rolling_mean) / rolling_std).astype("float64")
    prior_zscore = zscore.shift(1)
    entry_signal = (zscore <= (-1.0 * entry_z)) & ((prior_zscore > (-1.0 * entry_z)) | prior_zscore.isna())

    trades: list[ZScoreMeanReversionTrade] = []
    index = lookback
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
        exit_signal_row = None

        scan_index = entry_index
        while scan_index < len(frame) - 1:
            current_z = zscore.iloc[scan_index]
            holding_bars = scan_index - entry_index
            if not pd.isna(current_z) and float(current_z) >= (-1.0 * exit_z):
                exit_index = scan_index + 1
                exit_reason = EXIT_REASON_MEAN_REVERSION
                exit_signal_row = frame.iloc[scan_index]
                break
            if holding_bars >= max_hold_bars:
                exit_index = scan_index + 1
                exit_reason = EXIT_REASON_TIME_STOP
                exit_signal_row = frame.iloc[scan_index]
                break
            scan_index += 1

        if exit_index is None:
            exit_date = None
            exit_price = None
            realized_holding_bars = None
            return_pct = None
            status = "incomplete"
            last_entry_index = len(frame) - 1 if enforce_non_overlapping else last_entry_index
        else:
            exit_row = frame.iloc[exit_index]
            exit_date = exit_row[time_column]
            exit_price = float(exit_row["open"])
            realized_holding_bars = exit_index - entry_index
            return_pct = ((exit_price / entry_price) - 1.0) * 100.0
            status = "closed"
            last_entry_index = exit_index

        lookback_index = max(index - lookback + 1, 0)
        lookback_row = frame.iloc[lookback_index]
        signal_time = signal_row[time_column]
        signal_mean = float(rolling_mean.iloc[index]) if not pd.isna(rolling_mean.iloc[index]) else float(signal_row["close"])
        signal_std = float(rolling_std.iloc[index]) if not pd.isna(rolling_std.iloc[index]) else 0.0
        signal_z = float(zscore.iloc[index]) if not pd.isna(zscore.iloc[index]) else 0.0
        exit_trigger_mean = float(rolling_mean.iloc[scan_index]) if exit_signal_row is not None and not pd.isna(rolling_mean.iloc[scan_index]) else 0.0
        exit_trigger_z = float(zscore.iloc[scan_index]) if exit_signal_row is not None and not pd.isna(zscore.iloc[scan_index]) else 0.0
        std_pct = ((signal_std / signal_mean) * 100.0) if signal_mean else 0.0

        trades.append(
            ZScoreMeanReversionTrade(
                signal_id=f"{symbol}-{_format_signal_time(signal_time)}-{lookback}-{entry_z}-{exit_z}-{max_hold_bars}",
                symbol=symbol,
                wave_start_date=lookback_row[time_column],
                wave_end_date=signal_time,
                p0_price=round(signal_mean, 6),
                p1_price=round(float(signal_row["close"]), 6),
                p2_price=round(entry_price, 6),
                p3_price=round(exit_trigger_mean, 6),
                p4_price=round(exit_price, 6) if exit_price is not None else 0.0,
                p5_price=0.0,
                p0_date=lookback_row[time_column],
                p1_date=signal_time,
                p2_date=entry_row[time_column],
                p3_date=exit_signal_row[time_column] if exit_signal_row is not None else None,
                p4_date=exit_date,
                p5_date=None,
                wave_drop_pct=round(signal_z, 4),
                speed1=round(std_pct, 4),
                speed3=round(exit_trigger_z, 4),
                speed5=0.0,
                fractal_center_date=signal_time,
                signal_confirm_date=signal_time,
                entry_date=entry_row[time_column],
                entry_price=round(entry_price, 6),
                planned_hold_bars=max_hold_bars,
                exit_date=exit_date,
                exit_price=round(exit_price, 6) if exit_price is not None else None,
                return_pct=round(return_pct, 4) if return_pct is not None else None,
                holding_days=realized_holding_bars,
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

    return ZScoreMeanReversionTradeResult(trades=trades)


__all__ = [
    "DEFAULT_LOOKBACK",
    "DEFAULT_ENTRY_Z",
    "DEFAULT_EXIT_Z",
    "DEFAULT_MAX_HOLD_BARS",
    "EXIT_REASON_MEAN_REVERSION",
    "EXIT_REASON_TIME_STOP",
    "ZScoreMeanReversionTrade",
    "ZScoreMeanReversionTradeResult",
    "run_zscore_mean_reversion_backtest",
    "summarize_trade_results",
]
