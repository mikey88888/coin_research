from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from typing import Any

import pandas as pd

from ..strategies.five_wave_reversal import FiveWaveTrade


@dataclass(frozen=True)
class AccountConfig:
    initial_capital: float = 100000.0
    position_target_pct: float = 0.2
    max_positions: int = 5
    max_gross_exposure_pct: float = 1.0
    fee_rate: float = 0.001
    slippage_per_unit: float = 0.0
    quantity_step: float = 0.0001


@dataclass
class AccountPosition:
    trade: FiveWaveTrade
    quantity: float
    entry_time: pd.Timestamp
    entry_price: float
    entry_fee: float
    entry_slippage: float
    entry_notional: float
    equity_before_entry: float


@dataclass(frozen=True)
class AccountOrder:
    run_id: str
    timestamp: pd.Timestamp
    symbol: str
    side: str
    price: float
    quantity: float
    turnover: float
    fee: float
    slippage: float
    reason: str

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AccountEquityPoint:
    timestamp: pd.Timestamp
    cash: float
    market_value: float
    equity: float
    gross_exposure_pct: float
    drawdown_pct: float
    position_count: int

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AccountTradeRecord:
    run_id: str
    trade_id: str
    symbol: str
    quantity: float
    entry_date: Any
    exit_date: Any | None
    entry_price: float
    exit_price: float | None
    entry_notional: float
    exit_notional: float | None
    entry_fee: float
    exit_fee: float | None
    entry_slippage: float
    exit_slippage: float | None
    pnl_amount: float | None
    pnl_pct: float | None
    return_pct: float | None
    equity_before_entry: float
    equity_after_exit: float | None
    status: str
    exit_reason: str | None
    signal_id: str
    wave_start_date: Any
    wave_end_date: Any
    p0_date: Any
    p1_date: Any
    p2_date: Any
    p3_date: Any
    p4_date: Any
    p5_date: Any
    p0_price: float
    p1_price: float
    p2_price: float
    p3_price: float
    p4_price: float
    p5_price: float
    wave_drop_pct: float
    speed1: float
    speed3: float
    speed5: float
    fractal_center_date: Any
    signal_confirm_date: Any
    planned_hold_bars: int
    holding_days: int | None

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AccountBacktestResult:
    orders: list[AccountOrder]
    equity_curve: list[AccountEquityPoint]
    trades: list[AccountTradeRecord]
    summary: dict[str, Any]


def _to_timestamp(value: Any) -> pd.Timestamp:
    if isinstance(value, pd.Timestamp):
        return value
    return pd.Timestamp(value)


def _validate_account_config(config: AccountConfig) -> AccountConfig:
    if config.quantity_step <= 0:
        raise ValueError(f"quantity_step must be > 0, got {config.quantity_step}")
    return config


def _round_quantity(value: float, *, step: float) -> float:
    if value <= 0 or step <= 0:
        return 0.0
    units = math.floor(value / step)
    return round(units * step, 8)


def _normalize_market_frames(market_frames: dict[str, pd.DataFrame], *, time_column: str) -> tuple[dict[str, dict[pd.Timestamp, dict[str, Any]]], list[pd.Timestamp]]:
    frame_map: dict[str, dict[pd.Timestamp, dict[str, Any]]] = {}
    all_times: set[pd.Timestamp] = set()
    for symbol, frame in market_frames.items():
        if frame.empty:
            continue
        normalized = frame.loc[:, [time_column, "open", "close"]].copy()
        normalized[time_column] = pd.to_datetime(normalized[time_column], errors="coerce", utc=True)
        normalized["open"] = pd.to_numeric(normalized["open"], errors="coerce")
        normalized["close"] = pd.to_numeric(normalized["close"], errors="coerce")
        normalized = normalized.dropna(subset=[time_column, "open", "close"]).reset_index(drop=True)
        row_map = {
            pd.Timestamp(row[time_column]): {
                "open": float(row["open"]),
                "close": float(row["close"]),
            }
            for _, row in normalized.iterrows()
        }
        if not row_map:
            continue
        frame_map[symbol] = row_map
        all_times.update(row_map.keys())
    return frame_map, sorted(all_times)


def _normalize_signals(signals: list[FiveWaveTrade]) -> list[FiveWaveTrade]:
    return sorted(signals, key=lambda trade: (_to_timestamp(trade.entry_date), trade.symbol, trade.signal_id))


def _group_signals_by_entry_time(signals: list[FiveWaveTrade]) -> dict[pd.Timestamp, list[FiveWaveTrade]]:
    grouped: dict[pd.Timestamp, list[FiveWaveTrade]] = {}
    for signal in signals:
        grouped.setdefault(_to_timestamp(signal.entry_date), []).append(signal)
    for values in grouped.values():
        values.sort(key=lambda trade: (trade.symbol, trade.signal_id))
    return grouped


def _market_value(positions: dict[str, AccountPosition], marks: dict[str, float]) -> float:
    total = 0.0
    for symbol, position in positions.items():
        mark = marks.get(symbol, position.entry_price)
        total += position.quantity * mark
    return total


def _compute_equity(cash: float, positions: dict[str, AccountPosition], marks: dict[str, float]) -> tuple[float, float]:
    market_value = _market_value(positions, marks)
    return cash + market_value, market_value


def _execution_quantity(
    *,
    budget: float,
    execution_price: float,
    fee_rate: float,
    quantity_step: float,
) -> float:
    if execution_price <= 0 or budget <= 0:
        return 0.0
    gross_per_unit = execution_price * (1.0 + fee_rate)
    raw_quantity = budget / gross_per_unit
    return _round_quantity(raw_quantity, step=quantity_step)


def _build_trade_record(
    *,
    run_id: str,
    position: AccountPosition,
    exit_time: pd.Timestamp | None,
    exit_price: float | None,
    exit_fee: float | None,
    exit_slippage: float | None,
    equity_after_exit: float | None,
    status: str,
    exit_reason: str | None,
    holding_bars: int | None,
) -> AccountTradeRecord:
    exit_notional = None if exit_price is None else position.quantity * exit_price
    pnl_amount = None
    pnl_pct = None
    if exit_notional is not None and exit_fee is not None:
        pnl_amount = exit_notional - exit_fee - (position.entry_notional + position.entry_fee)
        denominator = position.entry_notional + position.entry_fee
        pnl_pct = (pnl_amount / denominator) * 100.0 if denominator else None

    trade = position.trade
    return AccountTradeRecord(
        run_id=run_id,
        trade_id=trade.signal_id,
        symbol=trade.symbol,
        quantity=position.quantity,
        entry_date=position.entry_time,
        exit_date=exit_time,
        entry_price=position.entry_price,
        exit_price=exit_price,
        entry_notional=position.entry_notional,
        exit_notional=exit_notional,
        entry_fee=position.entry_fee,
        exit_fee=exit_fee,
        entry_slippage=position.entry_slippage,
        exit_slippage=exit_slippage,
        pnl_amount=pnl_amount,
        pnl_pct=pnl_pct,
        return_pct=pnl_pct,
        equity_before_entry=position.equity_before_entry,
        equity_after_exit=equity_after_exit,
        status=status,
        exit_reason=exit_reason,
        signal_id=trade.signal_id,
        wave_start_date=trade.wave_start_date,
        wave_end_date=trade.wave_end_date,
        p0_date=trade.p0_date,
        p1_date=trade.p1_date,
        p2_date=trade.p2_date,
        p3_date=trade.p3_date,
        p4_date=trade.p4_date,
        p5_date=trade.p5_date,
        p0_price=trade.p0_price,
        p1_price=trade.p1_price,
        p2_price=trade.p2_price,
        p3_price=trade.p3_price,
        p4_price=trade.p4_price,
        p5_price=trade.p5_price,
        wave_drop_pct=trade.wave_drop_pct,
        speed1=trade.speed1,
        speed3=trade.speed3,
        speed5=trade.speed5,
        fractal_center_date=trade.fractal_center_date,
        signal_confirm_date=trade.signal_confirm_date,
        planned_hold_bars=trade.planned_hold_bars,
        holding_days=holding_bars,
    )


def run_account_backtest(
    *,
    run_id: str,
    signals: list[FiveWaveTrade],
    market_frames: dict[str, pd.DataFrame],
    time_column: str,
    config: AccountConfig,
) -> AccountBacktestResult:
    config = _validate_account_config(config)
    normalized_signals = _normalize_signals(signals)
    signals_by_time = _group_signals_by_entry_time(normalized_signals)
    frame_map, timeline = _normalize_market_frames(market_frames, time_column=time_column)

    cash = float(config.initial_capital)
    positions: dict[str, AccountPosition] = {}
    latest_close_marks: dict[str, float] = {}
    orders: list[AccountOrder] = []
    trades: list[AccountTradeRecord] = []
    equity_curve: list[AccountEquityPoint] = []
    peak_equity = float(config.initial_capital)

    for timestamp in timeline:
        current_rows = {symbol: rows[timestamp] for symbol, rows in frame_map.items() if timestamp in rows}
        open_marks = latest_close_marks.copy()
        for symbol, row in current_rows.items():
            open_marks[symbol] = float(row["open"])

        exiting_symbols = [
            symbol
            for symbol, position in positions.items()
            if position.trade.exit_date is not None and _to_timestamp(position.trade.exit_date) == timestamp
        ]
        for symbol in sorted(exiting_symbols):
            position = positions.pop(symbol)
            raw_exit_price = float(position.trade.exit_price or 0.0)
            execution_price = max(raw_exit_price - config.slippage_per_unit, 0.0)
            turnover = position.quantity * execution_price
            fee = turnover * config.fee_rate
            slippage_amount = position.quantity * config.slippage_per_unit
            cash += turnover - fee
            orders.append(
                AccountOrder(
                    run_id=run_id,
                    timestamp=timestamp,
                    symbol=symbol,
                    side="sell",
                    price=execution_price,
                    quantity=position.quantity,
                    turnover=turnover,
                    fee=fee,
                    slippage=slippage_amount,
                    reason=position.trade.exit_reason or "exit",
                )
            )
            latest_close_marks[symbol] = execution_price
            equity_after_exit, _ = _compute_equity(cash, positions, latest_close_marks)
            trades.append(
                _build_trade_record(
                    run_id=run_id,
                    position=position,
                    exit_time=timestamp,
                    exit_price=execution_price,
                    exit_fee=fee,
                    exit_slippage=slippage_amount,
                    equity_after_exit=equity_after_exit,
                    status="closed",
                    exit_reason=position.trade.exit_reason,
                    holding_bars=position.trade.holding_days,
                )
            )

        entry_candidates = signals_by_time.get(timestamp, [])
        for candidate in entry_candidates:
            if candidate.symbol in positions:
                continue
            if len(positions) >= config.max_positions:
                continue

            equity_before_entry, market_value_before_entry = _compute_equity(cash, positions, open_marks)
            target_notional = equity_before_entry * config.position_target_pct
            remaining_exposure = max(equity_before_entry * config.max_gross_exposure_pct - market_value_before_entry, 0.0)
            budget = min(target_notional, remaining_exposure, cash)
            execution_price = float(candidate.entry_price) + config.slippage_per_unit
            quantity = _execution_quantity(
                budget=budget,
                execution_price=execution_price,
                fee_rate=config.fee_rate,
                quantity_step=config.quantity_step,
            )
            if quantity <= 0:
                continue

            turnover = quantity * execution_price
            fee = turnover * config.fee_rate
            slippage_amount = quantity * config.slippage_per_unit
            total_cost = turnover + fee
            if total_cost > cash:
                continue

            cash -= total_cost
            position = AccountPosition(
                trade=candidate,
                quantity=quantity,
                entry_time=timestamp,
                entry_price=execution_price,
                entry_fee=fee,
                entry_slippage=slippage_amount,
                entry_notional=turnover,
                equity_before_entry=equity_before_entry,
            )
            positions[candidate.symbol] = position
            orders.append(
                AccountOrder(
                    run_id=run_id,
                    timestamp=timestamp,
                    symbol=candidate.symbol,
                    side="buy",
                    price=execution_price,
                    quantity=quantity,
                    turnover=turnover,
                    fee=fee,
                    slippage=slippage_amount,
                    reason="entry",
                )
            )

        for symbol, row in current_rows.items():
            latest_close_marks[symbol] = float(row["close"])

        equity, market_value = _compute_equity(cash, positions, latest_close_marks)
        peak_equity = max(peak_equity, equity)
        drawdown_pct = ((peak_equity - equity) / peak_equity) * 100.0 if peak_equity else 0.0
        gross_exposure_pct = (market_value / equity) * 100.0 if equity else 0.0
        equity_curve.append(
            AccountEquityPoint(
                timestamp=timestamp,
                cash=cash,
                market_value=market_value,
                equity=equity,
                gross_exposure_pct=gross_exposure_pct,
                drawdown_pct=drawdown_pct,
                position_count=len(positions),
            )
        )

    ending_equity = equity_curve[-1].equity if equity_curve else config.initial_capital
    ending_cash = equity_curve[-1].cash if equity_curve else config.initial_capital

    for symbol, position in positions.items():
        trades.append(
            _build_trade_record(
                run_id=run_id,
                position=position,
                exit_time=None,
                exit_price=None,
                exit_fee=None,
                exit_slippage=None,
                equity_after_exit=None,
                status="open",
                exit_reason=None,
                holding_bars=None,
            )
        )

    closed_trades = [trade for trade in trades if trade.status == "closed" and trade.pnl_pct is not None]
    pnl_pcts = pd.Series([trade.pnl_pct for trade in closed_trades], dtype="float64")
    holding_bars = pd.Series([trade.holding_days for trade in closed_trades], dtype="float64")
    total_fees_paid = float(sum(order.fee for order in orders))
    total_slippage_paid = float(sum(order.slippage for order in orders))
    total_trading_cost_paid = total_fees_paid + total_slippage_paid
    annualized_return_pct = None
    backtest_span_days = None
    if equity_curve and config.initial_capital:
        start_time = pd.Timestamp(equity_curve[0].timestamp)
        end_time = pd.Timestamp(equity_curve[-1].timestamp)
        span_days = (end_time - start_time).total_seconds() / 86400.0
        backtest_span_days = round(float(span_days), 4)
        span_years = span_days / 365.25
        if span_years > 0:
            annualized_return_pct = round((((ending_equity / config.initial_capital) ** (1.0 / span_years)) - 1.0) * 100.0, 4)

    summary = {
        "run_id": run_id,
        "engine_type": "account",
        "initial_capital": config.initial_capital,
        "ending_equity": round(float(ending_equity), 4),
        "ending_cash": round(float(ending_cash), 4),
        "total_return_pct": round(((ending_equity / config.initial_capital) - 1.0) * 100.0, 4) if config.initial_capital else None,
        "annualized_return_pct": annualized_return_pct,
        "max_drawdown_pct": round(max((point.drawdown_pct for point in equity_curve), default=0.0), 4),
        "backtest_span_days": backtest_span_days,
        "signals_found": len(signals),
        "trade_count": len(closed_trades),
        "closed_trades": len(closed_trades),
        "incomplete_trades": len([trade for trade in trades if trade.status != "closed"]),
        "win_rate": round(float((pnl_pcts > 0).mean() * 100), 4) if not pnl_pcts.empty else None,
        "avg_trade_return_pct": round(float(pnl_pcts.mean()), 4) if not pnl_pcts.empty else None,
        "median_trade_return_pct": round(float(pnl_pcts.median()), 4) if not pnl_pcts.empty else None,
        "avg_holding_bars": round(float(holding_bars.mean()), 4) if not holding_bars.empty else None,
        "avg_return_pct": round(float(pnl_pcts.mean()), 4) if not pnl_pcts.empty else None,
        "median_return_pct": round(float(pnl_pcts.median()), 4) if not pnl_pcts.empty else None,
        "avg_holding_days": round(float(holding_bars.mean()), 4) if not holding_bars.empty else None,
        "best_trade_pct": round(float(pnl_pcts.max()), 4) if not pnl_pcts.empty else None,
        "worst_trade_pct": round(float(pnl_pcts.min()), 4) if not pnl_pcts.empty else None,
        "stop_loss_count": sum(1 for trade in closed_trades if trade.exit_reason == "stop_loss"),
        "pattern_exit_count": sum(1 for trade in closed_trades if trade.exit_reason == "pattern_exit"),
        "time_exit_count": sum(1 for trade in closed_trades if trade.exit_reason == "time_exit"),
        "total_fees_paid": round(total_fees_paid, 4),
        "total_slippage_paid": round(total_slippage_paid, 4),
        "total_trading_cost_paid": round(total_trading_cost_paid, 4),
    }
    return AccountBacktestResult(orders=orders, equity_curve=equity_curve, trades=trades, summary=summary)
