from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path

import pandas as pd

from .backtests.account import AccountConfig, run_account_backtest
from .config import project_root
from .db import load_ohlcv, load_tracked_symbols
from .strategies.zscore_mean_reversion import (
    DEFAULT_ENTRY_Z,
    DEFAULT_EXIT_Z,
    DEFAULT_LOOKBACK,
    DEFAULT_MAX_HOLD_BARS,
    run_zscore_mean_reversion_backtest,
    summarize_trade_results,
)


STRATEGY_KEY = "zscore-mean-reversion"
TIMEFRAME_CHOICES = {"1d": "bar_time", "4h": "bar_time", "30m": "bar_time"}
ENGINE_CHOICES = {"account", "signal"}
EXIT_MODE = "mean_reversion_time_stop"


def _parse_symbols_arg(value: str | None) -> list[str] | None:
    if not value:
        return None
    items = [part.strip().upper() for part in value.split(",") if part.strip()]
    return items or None


def _slug_float(value: float) -> str:
    text = f"{value:.2f}".rstrip("0").rstrip(".")
    return text.replace("-", "m").replace(".", "p")


def _run_id(*, timeframe: str, lookback: int, entry_z: float, exit_z: float, max_hold_bars: int) -> str:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}__{timeframe}__lb{lookback}_ez{_slug_float(entry_z)}_xz{_slug_float(exit_z)}_h{max_hold_bars}"


def _run_dir(root: Path, *, run_id: str) -> Path:
    return root / "reports" / "backtests" / STRATEGY_KEY / run_id


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_frame_for_timeframe(symbol: str, *, exchange_name: str, timeframe: str, dsn: str | None = None) -> pd.DataFrame:
    frame = load_ohlcv(exchange_name=exchange_name, symbol=symbol, timeframe=timeframe, dsn=dsn)
    if frame.empty:
        return frame
    return frame.loc[:, ["symbol", "bar_time", "open", "high", "low", "close", "volume", "source", "fetched_at"]].copy()


def _return_drawdown_ratio(summary: dict[str, float | int | None]) -> float | None:
    annualized = summary.get("annualized_return_pct")
    max_drawdown = summary.get("max_drawdown_pct")
    if annualized is None or max_drawdown in (None, 0):
        return None
    return round(float(annualized) / float(max_drawdown), 4)


def run_backtest(
    *,
    exchange_name: str,
    engine: str,
    timeframe: str,
    lookback: int,
    entry_z: float,
    exit_z: float,
    max_hold_bars: int,
    symbols: list[str] | None = None,
    initial_capital: float = 100000.0,
    position_target_pct: float = 0.2,
    max_positions: int = 5,
    max_gross_exposure_pct: float = 1.0,
    fee_rate: float = 0.001,
    slippage_per_unit: float = 0.0,
    quantity_step: float = 0.0001,
    root: Path | None = None,
    dsn: str | None = None,
) -> tuple[pd.DataFrame, dict[str, float | int | None], Path, dict[str, object]]:
    if engine not in ENGINE_CHOICES:
        raise ValueError(f"unsupported engine: {engine}")
    if timeframe not in TIMEFRAME_CHOICES:
        raise ValueError(f"unsupported timeframe: {timeframe}")

    available_symbols = load_tracked_symbols(exchange_name=exchange_name, timeframe=timeframe, dsn=dsn)
    available_symbol_set = set(available_symbols)
    target_symbols = available_symbols if symbols is None else [symbol for symbol in symbols if symbol in available_symbol_set]
    missing_symbols = [] if symbols is None else sorted(set(symbols) - set(target_symbols))
    if missing_symbols:
        raise ValueError(f"symbols not found in synced crypto universe: {missing_symbols}")

    run_id = _run_id(timeframe=timeframe, lookback=lookback, entry_z=entry_z, exit_z=exit_z, max_hold_bars=max_hold_bars)
    root_dir = root or project_root()
    run_dir = _run_dir(root_dir, run_id=run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    trades = []
    market_frames: dict[str, pd.DataFrame] = {}
    for symbol in target_symbols:
        frame = _load_frame_for_timeframe(symbol, exchange_name=exchange_name, timeframe=timeframe, dsn=dsn)
        if frame.empty:
            continue
        result = run_zscore_mean_reversion_backtest(
            frame,
            symbol=symbol,
            lookback=lookback,
            entry_z=entry_z,
            exit_z=exit_z,
            max_hold_bars=max_hold_bars,
            time_column=TIMEFRAME_CHOICES[timeframe],
            enforce_non_overlapping=(engine == "signal"),
        )
        trades.extend(result.trades)
        if engine == "account":
            market_frames[symbol] = frame.loc[:, [TIMEFRAME_CHOICES[timeframe], "open", "close"]].copy()

    if engine == "account":
        account_result = run_account_backtest(
            run_id=run_id,
            signals=trades,
            market_frames=market_frames,
            time_column=TIMEFRAME_CHOICES[timeframe],
            config=AccountConfig(
                initial_capital=initial_capital,
                position_target_pct=position_target_pct,
                max_positions=max_positions,
                max_gross_exposure_pct=max_gross_exposure_pct,
                fee_rate=fee_rate,
                slippage_per_unit=slippage_per_unit,
                quantity_step=quantity_step,
            ),
        )
        trade_frame = (
            pd.DataFrame([trade.to_record() for trade in account_result.trades])
            .sort_values(["entry_date", "symbol"], na_position="last")
            .reset_index(drop=True)
            if account_result.trades
            else pd.DataFrame()
        )
        orders_frame = pd.DataFrame([order.to_record() for order in account_result.orders])
        equity_curve_frame = pd.DataFrame([point.to_record() for point in account_result.equity_curve])
        summary = dict(account_result.summary)
        summary["return_drawdown_ratio"] = _return_drawdown_ratio(summary)
    else:
        trade_frame = pd.DataFrame([trade.to_record() for trade in trades])
        if trade_frame.empty:
            trade_frame = pd.DataFrame(columns=["signal_id", "symbol", "entry_date", "exit_date", "return_pct", "status", "exit_reason"])
        else:
            trade_frame = trade_frame.sort_values(["entry_date", "symbol"]).reset_index(drop=True)
        orders_frame = pd.DataFrame()
        equity_curve_frame = pd.DataFrame()
        summary = summarize_trade_results(trades, universe_symbols=len(target_symbols))

    trades_path = run_dir / "trades.csv"
    summary_path = run_dir / "summary.json"
    meta_path = run_dir / "run_meta.json"
    trade_frame.to_csv(trades_path, index=False)
    if engine == "account":
        orders_path = run_dir / "orders.csv"
        equity_curve_path = run_dir / "equity_curve.csv"
        orders_frame.to_csv(orders_path, index=False)
        equity_curve_frame.to_csv(equity_curve_path, index=False)
    else:
        orders_path = None
        equity_curve_path = None

    now = datetime.now().isoformat(timespec="seconds")
    meta = {
        "run_id": run_id,
        "strategy_key": STRATEGY_KEY,
        "strategy_label": "Z-score Mean Reversion",
        "engine_type": engine,
        "timeframe": timeframe,
        "exit_mode": EXIT_MODE,
        "exchange": exchange_name,
        "params": {
            "lookback": lookback,
            "entry_z": entry_z,
            "exit_z": exit_z,
            "max_hold_bars": max_hold_bars,
            "quantity_step": quantity_step if engine == "account" else None,
        },
        "universe": "market_data.crypto_ohlcv",
        "data_source": f"market_data.crypto_ohlcv:{exchange_name}:{timeframe}",
        "initial_capital": initial_capital if engine == "account" else None,
        "position_target_pct": position_target_pct if engine == "account" else None,
        "max_positions": max_positions if engine == "account" else None,
        "max_gross_exposure_pct": max_gross_exposure_pct if engine == "account" else None,
        "fee_rate": fee_rate if engine == "account" else None,
        "slippage_per_unit": slippage_per_unit if engine == "account" else None,
        "quantity_step": quantity_step if engine == "account" else None,
        "started_at": now,
        "finished_at": now,
        "summary_path": str(summary_path),
        "trades_path": str(trades_path),
        "orders_path": str(orders_path) if orders_path else None,
        "equity_curve_path": str(equity_curve_path) if equity_curve_path else None,
    }
    summary_payload = {**summary, "run_id": run_id}
    _write_json(summary_path, summary_payload)
    _write_json(meta_path, meta)
    return trade_frame, summary, run_dir, meta


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest a minimal z-score mean reversion prototype on crypto data")
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--symbols", help="Comma separated market symbols, for example BTC/USDT,ETH/USDT")
    parser.add_argument("--engine", choices=sorted(ENGINE_CHOICES), default="account")
    parser.add_argument("--timeframe", choices=sorted(TIMEFRAME_CHOICES), default="4h")
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK)
    parser.add_argument("--entry-z", type=float, default=DEFAULT_ENTRY_Z)
    parser.add_argument("--exit-z", type=float, default=DEFAULT_EXIT_Z)
    parser.add_argument("--max-hold-bars", type=int, default=DEFAULT_MAX_HOLD_BARS)
    parser.add_argument("--initial-capital", type=float, default=100000.0)
    parser.add_argument("--position-target-pct", type=float, default=0.2)
    parser.add_argument("--max-positions", type=int, default=5)
    parser.add_argument("--max-gross-exposure-pct", type=float, default=1.0)
    parser.add_argument("--fee-rate", type=float, default=0.001)
    parser.add_argument("--slippage-per-unit", type=float, default=0.0)
    parser.add_argument("--quantity-step", type=float, default=0.0001)
    args = parser.parse_args()

    symbols = _parse_symbols_arg(args.symbols)
    trade_frame, summary, run_dir, _ = run_backtest(
        exchange_name=args.exchange,
        engine=args.engine,
        timeframe=args.timeframe,
        lookback=args.lookback,
        entry_z=args.entry_z,
        exit_z=args.exit_z,
        max_hold_bars=args.max_hold_bars,
        symbols=symbols,
        initial_capital=args.initial_capital,
        position_target_pct=args.position_target_pct,
        max_positions=args.max_positions,
        max_gross_exposure_pct=args.max_gross_exposure_pct,
        fee_rate=args.fee_rate,
        slippage_per_unit=args.slippage_per_unit,
        quantity_step=args.quantity_step,
    )

    for key in (
        "signals_found",
        "closed_trades",
        "incomplete_trades",
        "win_rate",
        "avg_return_pct",
        "median_return_pct",
        "avg_holding_days",
        "best_trade_pct",
        "worst_trade_pct",
        "total_return_pct",
        "annualized_return_pct",
        "max_drawdown_pct",
        "return_drawdown_ratio",
    ):
        print(f"{key}={summary.get(key)}")
    print(f"rows={len(trade_frame)}")
    print(f"run_dir={run_dir}")


if __name__ == "__main__":
    main()
