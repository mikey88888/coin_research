from __future__ import annotations

import math
from typing import Any

import pandas as pd

from .account import (
    AccountBacktestResult,
    AccountConfig,
    AccountEquityPoint,
    AccountOrder,
    AccountPosition,
    AccountTradeRecord,
    _normalize_market_frames,
    _normalize_signals,
    _to_timestamp,
    _validate_account_config,
)
from ..strategies.five_wave_reversal import FiveWaveTrade


def _round_quantity(value: float, *, step: float) -> float:
    if value <= 0 or step <= 0:
        return 0.0
    units = math.floor(value / step)
    return round(units * step, 8)


def _short_liability(positions: dict[str, AccountPosition], marks: dict[str, float]) -> float:
    total = 0.0
    for symbol, position in positions.items():
        mark = marks.get(symbol, position.entry_price)
        total += position.quantity * mark
    return total


def _compute_short_equity(cash: float, positions: dict[str, AccountPosition], marks: dict[str, float]) -> tuple[float, float]:
    liability = _short_liability(positions, marks)
    return cash - liability, liability


def _execution_quantity(
    *,
    target_notional: float,
    execution_price: float,
    quantity_step: float,
) -> float:
    if execution_price <= 0 or target_notional <= 0:
        return 0.0
    return _round_quantity(target_notional / execution_price, step=quantity_step)


def _build_short_trade_record(
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
        pnl_amount = position.entry_notional - position.entry_fee - exit_notional - exit_fee
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


def run_short_account_backtest(
    *,
    run_id: str,
    signals: list[FiveWaveTrade],
    market_frames: dict[str, pd.DataFrame],
    time_column: str,
    config: AccountConfig,
) -> AccountBacktestResult:
    config = _validate_account_config(config)
    normalized_signals = _normalize_signals(signals)
    signals_by_time = {}
    for signal in normalized_signals:
        signals_by_time.setdefault(_to_timestamp(signal.entry_date), []).append(signal)
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
            execution_price = raw_exit_price + config.slippage_per_unit
            turnover = position.quantity * execution_price
            fee = turnover * config.fee_rate
            slippage_amount = position.quantity * config.slippage_per_unit
            cash -= turnover + fee
            orders.append(
                AccountOrder(
                    run_id=run_id,
                    timestamp=timestamp,
                    symbol=symbol,
                    side="buy_to_cover",
                    price=execution_price,
                    quantity=position.quantity,
                    turnover=turnover,
                    fee=fee,
                    slippage=slippage_amount,
                    reason=position.trade.exit_reason or "exit",
                )
            )
            latest_close_marks[symbol] = execution_price
            equity_after_exit, _ = _compute_short_equity(cash, positions, latest_close_marks)
            trades.append(
                _build_short_trade_record(
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

            equity_before_entry, liability_before_entry = _compute_short_equity(cash, positions, open_marks)
            if equity_before_entry <= 0:
                continue
            target_notional = equity_before_entry * config.position_target_pct
            remaining_exposure = max(equity_before_entry * config.max_gross_exposure_pct - liability_before_entry, 0.0)
            short_notional = min(target_notional, remaining_exposure)
            execution_price = max(float(candidate.entry_price) - config.slippage_per_unit, 0.0)
            quantity = _execution_quantity(
                target_notional=short_notional,
                execution_price=execution_price,
                quantity_step=config.quantity_step,
            )
            if quantity <= 0:
                continue

            turnover = quantity * execution_price
            fee = turnover * config.fee_rate
            slippage_amount = quantity * config.slippage_per_unit
            cash += turnover - fee
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
                    side="sell_short",
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

        equity, liability = _compute_short_equity(cash, positions, latest_close_marks)
        peak_equity = max(peak_equity, equity)
        drawdown_pct = ((peak_equity - equity) / peak_equity) * 100.0 if peak_equity else 0.0
        gross_exposure_pct = (liability / equity) * 100.0 if equity else 0.0
        equity_curve.append(
            AccountEquityPoint(
                timestamp=timestamp,
                cash=cash,
                market_value=-liability,
                equity=equity,
                gross_exposure_pct=gross_exposure_pct,
                drawdown_pct=drawdown_pct,
                position_count=len(positions),
            )
        )

    ending_equity = equity_curve[-1].equity if equity_curve else config.initial_capital
    ending_cash = equity_curve[-1].cash if equity_curve else config.initial_capital

    for position in positions.values():
        trades.append(
            _build_short_trade_record(
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
    annualized_return_pct = None
    backtest_span_days = None
    if equity_curve and config.initial_capital:
        start_time = pd.Timestamp(equity_curve[0].timestamp)
        end_time = pd.Timestamp(equity_curve[-1].timestamp)
        span_days = (end_time - start_time).total_seconds() / 86400.0
        backtest_span_days = round(float(span_days), 4)
        span_years = span_days / 365.25
        if span_years > 0 and ending_equity > 0:
            annualized_return_pct = round((((ending_equity / config.initial_capital) ** (1.0 / span_years)) - 1.0) * 100.0, 4)

    summary = {
        "run_id": run_id,
        "engine_type": "short_account",
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
        "total_trading_cost_paid": round(total_fees_paid + total_slippage_paid, 4),
    }
    return AccountBacktestResult(orders=orders, equity_curve=equity_curve, trades=trades, summary=summary)


__all__ = ["run_short_account_backtest"]
