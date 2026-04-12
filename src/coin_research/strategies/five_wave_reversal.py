from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd


REVERSAL_PCT = 0.05
MIN_WAVE_DROP_PCT = -10.0
MIN_WAVE_BAR_COUNT = 5
DEFAULT_TRAILING_STOP_PCT = 0.15
EXIT_MODE_TIME_ONLY = "time_only"
EXIT_MODE_TRAILING_STOP = "trailing_stop"
EXIT_MODE_THREE_WAVE = "three_wave_exit"


@dataclass(frozen=True)
class Pivot:
    kind: str
    index: int
    trade_date: Any
    price: float


@dataclass(frozen=True)
class FiveWaveTrade:
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
class FiveWaveTradeResult:
    trades: list[FiveWaveTrade]
    pivots: list[Pivot]


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


def _make_pivot(frame: pd.DataFrame, index: int, kind: str, *, time_column: str) -> Pivot:
    price_column = "high" if kind == "high" else "low"
    row = frame.iloc[index]
    return Pivot(
        kind=kind,
        index=int(index),
        trade_date=row[time_column],
        price=float(row[price_column]),
    )


def _append_pivot(pivots: list[Pivot], pivot: Pivot) -> None:
    if pivots and pivots[-1].index == pivot.index:
        if pivots[-1].kind == pivot.kind:
            pivots[-1] = pivot
        return
    if pivots and pivots[-1].kind == pivot.kind:
        previous = pivots[-1]
        if pivot.kind == "high" and pivot.price >= previous.price:
            pivots[-1] = pivot
        elif pivot.kind == "low" and pivot.price <= previous.price:
            pivots[-1] = pivot
        return
    pivots.append(pivot)


def build_zigzag_pivots(
    df: pd.DataFrame,
    reversal_pct: float = REVERSAL_PCT,
    *,
    time_column: str = "bar_time",
) -> list[Pivot]:
    frame = _prepare_frame(df, time_column=time_column)
    if len(frame) < 2:
        return []

    highs = frame["high"].tolist()
    lows = frame["low"].tolist()
    pivots: list[Pivot] = []

    trend: str | None = None
    candidate_high_idx = 0
    candidate_high = float(highs[0])
    candidate_low_idx = 0
    candidate_low = float(lows[0])

    for index in range(1, len(frame)):
        high = float(highs[index])
        low = float(lows[index])

        if trend is None:
            if high >= candidate_high:
                candidate_high = high
                candidate_high_idx = index
            if low <= candidate_low:
                candidate_low = low
                candidate_low_idx = index

            drop_from_high = (low / candidate_high) - 1.0
            rise_from_low = (high / candidate_low) - 1.0
            if drop_from_high <= -reversal_pct and candidate_high_idx < index:
                _append_pivot(pivots, _make_pivot(frame, candidate_high_idx, "high", time_column=time_column))
                trend = "down"
                candidate_low_idx = index
                candidate_low = low
            elif rise_from_low >= reversal_pct and candidate_low_idx < index:
                _append_pivot(pivots, _make_pivot(frame, candidate_low_idx, "low", time_column=time_column))
                trend = "up"
                candidate_high_idx = index
                candidate_high = high
            continue

        if trend == "down":
            if low <= candidate_low:
                candidate_low = low
                candidate_low_idx = index
            rebound = (high / candidate_low) - 1.0
            if rebound >= reversal_pct and candidate_low_idx < index:
                _append_pivot(pivots, _make_pivot(frame, candidate_low_idx, "low", time_column=time_column))
                trend = "up"
                candidate_high_idx = index
                candidate_high = high
            continue

        if high >= candidate_high:
            candidate_high = high
            candidate_high_idx = index
        retrace = (low / candidate_high) - 1.0
        if retrace <= -reversal_pct and candidate_high_idx < index:
            _append_pivot(pivots, _make_pivot(frame, candidate_high_idx, "high", time_column=time_column))
            trend = "down"
            candidate_low_idx = index
            candidate_low = low

    if trend == "down":
        _append_pivot(pivots, _make_pivot(frame, candidate_low_idx, "low", time_column=time_column))
    elif trend == "up":
        _append_pivot(pivots, _make_pivot(frame, candidate_high_idx, "high", time_column=time_column))

    return pivots


def _bars_between(left: Pivot, right: Pivot) -> int:
    return max(int(right.index - left.index), 1)


def _bars_in_wave(left: Pivot, right: Pivot) -> int:
    return max(int(right.index - left.index) + 1, 1)


def _wave_drop_pct(p0: Pivot, p5: Pivot) -> float:
    return ((p5.price / p0.price) - 1.0) * 100.0


def _find_bottom_fractal(frame: pd.DataFrame, start_index: int) -> tuple[int, int, int] | None:
    if len(frame) < 3:
        return None
    for center_index in range(max(start_index, 1), len(frame) - 1):
        left_low = float(frame.iloc[center_index - 1]["low"])
        center_low = float(frame.iloc[center_index]["low"])
        right_low = float(frame.iloc[center_index + 1]["low"])
        if center_low < left_low and center_low < right_low:
            confirm_index = center_index + 1
            entry_index = confirm_index + 1
            if entry_index >= len(frame):
                return None
            return center_index, confirm_index, entry_index
    return None


def _find_top_fractal(frame: pd.DataFrame, start_index: int) -> tuple[int, int, int] | None:
    if len(frame) < 3:
        return None
    for center_index in range(max(start_index, 1), len(frame) - 1):
        left_high = float(frame.iloc[center_index - 1]["high"])
        center_high = float(frame.iloc[center_index]["high"])
        right_high = float(frame.iloc[center_index + 1]["high"])
        if center_high > left_high and center_high > right_high:
            confirm_index = center_index + 1
            exit_index = confirm_index + 1
            if exit_index >= len(frame):
                return None
            return center_index, confirm_index, exit_index
    return None


def _map_pivots_to_global(frame: pd.DataFrame, *, entry_index: int, reversal_pct: float, time_column: str) -> list[Pivot]:
    subframe = frame.iloc[entry_index:].reset_index(drop=True)
    pivots = build_zigzag_pivots(subframe, reversal_pct=reversal_pct, time_column=time_column)
    mapped = [
        Pivot(kind=pivot.kind, index=pivot.index + entry_index, trade_date=pivot.trade_date, price=pivot.price)
        for pivot in pivots
    ]
    if not mapped or mapped[0].kind == "high":
        entry_row = frame.iloc[entry_index]
        mapped.insert(
            0,
            Pivot(
                kind="low",
                index=entry_index,
                trade_date=entry_row[time_column],
                price=float(entry_row["low"]),
            ),
        )
    return mapped


def _find_three_wave_up_exit(
    frame: pd.DataFrame,
    *,
    entry_index: int,
    max_exit_index: int,
    reversal_pct: float,
    time_column: str,
) -> tuple[Any | None, float | None, int | None, str | None, int | None]:
    pivots = _map_pivots_to_global(frame, entry_index=entry_index, reversal_pct=reversal_pct, time_column=time_column)
    for start in range(0, max(len(pivots) - 5, 0)):
        window = pivots[start : start + 6]
        if len(window) < 6:
            continue
        if [pivot.kind for pivot in window] != ["low", "high", "low", "high", "low", "high"]:
            continue
        p0, p1, p2, p3, p4, p5 = window
        if p0.index < entry_index:
            continue
        if not (p0.price < p2.price < p4.price):
            continue
        if not (p1.price < p3.price < p5.price):
            continue
        fractal = _find_top_fractal(frame, p5.index)
        if fractal is None:
            continue
        _, _, exit_index = fractal
        if exit_index > max_exit_index:
            continue
        row = frame.iloc[exit_index]
        return row[time_column], float(row["open"]), exit_index - entry_index, "pattern_exit", exit_index
    return None, None, None, None, None


def _find_trailing_stop_exit(
    frame: pd.DataFrame,
    *,
    entry_index: int,
    planned_exit_index: int,
    entry_price: float,
    trailing_stop_pct: float,
    time_column: str,
) -> tuple[Any | None, float | None, int | None, str | None, int | None]:
    trailing_peak = entry_price
    for index in range(entry_index, min(planned_exit_index, len(frame))):
        row = frame.iloc[index]
        day_open = float(row["open"])
        day_high = float(row["high"])
        day_low = float(row["low"])
        stop_price = trailing_peak * (1.0 - trailing_stop_pct)
        if day_open <= stop_price:
            return row[time_column], day_open, index - entry_index, "stop_loss", index
        if day_low < stop_price:
            return row[time_column], stop_price, index - entry_index, "stop_loss", index
        trailing_peak = max(trailing_peak, day_high)
    return None, None, None, None, None


def _resolve_exit(
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
        exit_date, exit_price, holding_bars, exit_reason, exit_index = _find_trailing_stop_exit(
            frame,
            entry_index=entry_index,
            planned_exit_index=planned_exit_index,
            entry_price=entry_price,
            trailing_stop_pct=trailing_stop_pct,
            time_column=time_column,
        )
        if exit_index is not None:
            return exit_date, exit_price, holding_bars, exit_reason, exit_index, "closed"
    elif exit_mode == EXIT_MODE_THREE_WAVE:
        exit_date, exit_price, holding_bars, exit_reason, exit_index = _find_three_wave_up_exit(
            frame,
            entry_index=entry_index,
            max_exit_index=max_exit_index,
            reversal_pct=reversal_pct,
            time_column=time_column,
        )
        if exit_index is not None:
            return exit_date, exit_price, holding_bars, exit_reason, exit_index, "closed"

    if planned_exit_index < len(frame):
        row = frame.iloc[planned_exit_index]
        return row[time_column], float(row["open"]), planned_exit_index - entry_index, "time_exit", planned_exit_index, "closed"
    return None, None, None, None, None, "incomplete"


def _build_trade(
    frame: pd.DataFrame,
    symbol: str,
    pivots: list[Pivot],
    *,
    reversal_pct: float,
    time_column: str,
    exit_mode: str,
    trailing_stop_pct: float,
) -> FiveWaveTrade | None:
    p0, p1, p2, p3, p4, p5 = pivots
    if [pivot.kind for pivot in pivots] != ["high", "low", "high", "low", "high", "low"]:
        return None
    if not all(
        _bars_in_wave(left, right) >= MIN_WAVE_BAR_COUNT
        for left, right in ((p0, p1), (p1, p2), (p2, p3), (p3, p4), (p4, p5))
    ):
        return None
    if not (p0.price > p2.price > p4.price):
        return None
    if not (p1.price > p3.price > p5.price):
        return None

    wave_drop_pct = _wave_drop_pct(p0, p5)
    if wave_drop_pct > MIN_WAVE_DROP_PCT:
        return None

    speed1 = (p0.price - p1.price) / _bars_between(p0, p1)
    speed3 = (p2.price - p3.price) / _bars_between(p2, p3)
    speed5 = (p4.price - p5.price) / _bars_between(p4, p5)
    if not (speed5 > speed1 and speed5 > speed3):
        return None

    fractal = _find_bottom_fractal(frame, p5.index)
    if fractal is None:
        return None
    fractal_center_index, confirm_index, entry_index = fractal

    hold_bars = max(_bars_in_wave(p0, p5), 1)
    planned_exit_index = entry_index + hold_bars
    entry_row = frame.iloc[entry_index]
    entry_price = float(entry_row["open"])
    exit_date, exit_price, holding_bars, exit_reason, resolved_exit_index, status = _resolve_exit(
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

    return FiveWaveTrade(
        signal_id=f"{symbol}-{_format_signal_time(p0.trade_date)}-{_format_signal_time(entry_row[time_column])}",
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
        wave_drop_pct=round(wave_drop_pct, 4),
        speed1=round(speed1, 6),
        speed3=round(speed3, 6),
        speed5=round(speed5, 6),
        fractal_center_date=frame.iloc[fractal_center_index][time_column],
        signal_confirm_date=frame.iloc[confirm_index][time_column],
        entry_date=entry_row[time_column],
        entry_price=round(entry_price, 6),
        planned_hold_bars=hold_bars,
        exit_date=exit_date,
        exit_price=None if exit_price is None else round(exit_price, 6),
        return_pct=None if return_pct is None else round(return_pct, 4),
        holding_days=holding_bars,
        status=status,
        exit_reason=exit_reason,
        p0_index=p0.index,
        p5_index=p5.index,
        entry_index=entry_index,
        exit_index=resolved_exit_index,
    )


def run_five_wave_reversal_backtest(
    df: pd.DataFrame,
    *,
    symbol: str,
    reversal_pct: float = REVERSAL_PCT,
    time_column: str = "bar_time",
    exit_mode: str = EXIT_MODE_THREE_WAVE,
    trailing_stop_pct: float = DEFAULT_TRAILING_STOP_PCT,
    enforce_non_overlapping: bool = True,
) -> FiveWaveTradeResult:
    frame = _prepare_frame(df, time_column=time_column)
    pivots = build_zigzag_pivots(frame, reversal_pct=reversal_pct, time_column=time_column)
    trades: list[FiveWaveTrade] = []
    skip_until_index = -1

    for start in range(0, max(len(pivots) - 5, 0)):
        window = pivots[start : start + 6]
        if len(window) < 6:
            continue
        trade = _build_trade(
            frame,
            symbol,
            window,
            reversal_pct=reversal_pct,
            time_column=time_column,
            exit_mode=exit_mode,
            trailing_stop_pct=trailing_stop_pct,
        )
        if trade is None:
            continue
        if enforce_non_overlapping and trade.entry_index <= skip_until_index:
            continue
        trades.append(trade)
        if enforce_non_overlapping:
            skip_until_index = trade.exit_index if trade.exit_index is not None else len(frame) - 1

    return FiveWaveTradeResult(trades=trades, pivots=pivots)


def summarize_trade_results(trades: list[FiveWaveTrade], *, universe_symbols: int) -> dict[str, float | int | None]:
    closed = [trade for trade in trades if trade.status == "closed" and trade.return_pct is not None]
    returns = [float(trade.return_pct) for trade in closed]
    holding_days = [int(trade.holding_days or 0) for trade in closed]
    exit_reasons = [trade.exit_reason for trade in closed]
    return {
        "universe_symbols": universe_symbols,
        "signals_found": len(trades),
        "closed_trades": len(closed),
        "incomplete_trades": len(trades) - len(closed),
        "win_rate": round((sum(1 for value in returns if value > 0) / len(returns) * 100.0), 4) if returns else None,
        "avg_return_pct": round(sum(returns) / len(returns), 4) if returns else None,
        "median_return_pct": round(float(pd.Series(returns).median()), 4) if returns else None,
        "avg_holding_days": round(sum(holding_days) / len(holding_days), 4) if holding_days else None,
        "best_trade_pct": round(max(returns), 4) if returns else None,
        "worst_trade_pct": round(min(returns), 4) if returns else None,
        "stop_loss_count": sum(1 for reason in exit_reasons if reason == "stop_loss"),
        "pattern_exit_count": sum(1 for reason in exit_reasons if reason == "pattern_exit"),
        "time_exit_count": sum(1 for reason in exit_reasons if reason == "time_exit"),
    }


__all__ = [
    "DEFAULT_TRAILING_STOP_PCT",
    "EXIT_MODE_THREE_WAVE",
    "EXIT_MODE_TIME_ONLY",
    "EXIT_MODE_TRAILING_STOP",
    "FiveWaveTrade",
    "FiveWaveTradeResult",
    "Pivot",
    "build_zigzag_pivots",
    "run_five_wave_reversal_backtest",
    "summarize_trade_results",
]
