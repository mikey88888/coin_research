from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path

import pandas as pd

from .backtests import AccountConfig, run_short_account_backtest
from .backtest_absolute_momentum_volatility_composite import (
    TIMEFRAME_CHOICES,
    _decimal_slug,
    _load_frame_for_timeframe,
    _return_drawdown_ratio,
    _write_json,
)
from .config import project_root
from .db import load_tracked_symbols
from .strategies.absolute_momentum_volatility_composite import (
    DEFAULT_HOLD_BARS,
    DEFAULT_LOOKBACK_BARS,
    DEFAULT_MIN_MOMENTUM_PCT,
    DEFAULT_MIN_VOLATILITY_PCT,
    DEFAULT_REBALANCE_INTERVAL,
    DEFAULT_TOP_K,
    DEFAULT_VOLATILITY_WINDOW,
    run_absolute_momentum_volatility_composite_backtest,
)


STRATEGY_KEY = "absolute-momentum-volatility-composite-short"


def _run_id(
    *,
    timeframe: str,
    lookback_bars: int,
    volatility_window: int,
    top_k: int,
    hold_bars: int,
    rebalance_interval: int,
    min_volatility_pct: float,
    min_momentum_pct: float,
) -> str:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return (
        f"{timestamp}__{timeframe}__short_lb{lookback_bars}_vw{volatility_window}_top{top_k}"
        f"_h{hold_bars}_rb{rebalance_interval}_mv{_decimal_slug(min_volatility_pct)}"
        f"_am{_decimal_slug(min_momentum_pct)}"
    )


def _run_dir(root: Path, *, run_id: str) -> Path:
    return root / "reports" / "backtests" / STRATEGY_KEY / run_id


def run_backtest(
    *,
    exchange_name: str,
    timeframe: str,
    lookback_bars: int,
    volatility_window: int,
    hold_bars: int,
    top_k: int,
    rebalance_interval: int,
    min_universe_size: int | None = None,
    min_volatility_pct: float = DEFAULT_MIN_VOLATILITY_PCT,
    min_momentum_pct: float = DEFAULT_MIN_MOMENTUM_PCT,
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
    if timeframe not in TIMEFRAME_CHOICES:
        raise ValueError(f"unsupported timeframe: {timeframe}")

    available_symbols = load_tracked_symbols(exchange_name=exchange_name, timeframe=timeframe, dsn=dsn)
    available_symbol_set = set(available_symbols)
    target_symbols = available_symbols if symbols is None else [symbol for symbol in symbols if symbol in available_symbol_set]
    missing_symbols = [] if symbols is None else sorted(set(symbols) - set(target_symbols))
    if missing_symbols:
        raise ValueError(f"symbols not found in synced crypto universe: {missing_symbols}")

    run_id = _run_id(
        timeframe=timeframe,
        lookback_bars=lookback_bars,
        volatility_window=volatility_window,
        top_k=top_k,
        hold_bars=hold_bars,
        rebalance_interval=rebalance_interval,
        min_volatility_pct=min_volatility_pct,
        min_momentum_pct=min_momentum_pct,
    )
    root_dir = root or project_root()
    run_dir = _run_dir(root_dir, run_id=run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    market_frames: dict[str, pd.DataFrame] = {}
    for symbol in target_symbols:
        frame = _load_frame_for_timeframe(symbol, exchange_name=exchange_name, timeframe=timeframe, dsn=dsn)
        if frame.empty:
            continue
        market_frames[symbol] = frame

    signal_result = run_absolute_momentum_volatility_composite_backtest(
        market_frames,
        lookback_bars=lookback_bars,
        volatility_window=volatility_window,
        hold_bars=hold_bars,
        top_k=top_k,
        rebalance_interval=rebalance_interval,
        min_universe_size=min_universe_size,
        min_volatility_pct=min_volatility_pct,
        min_momentum_pct=min_momentum_pct,
        time_column=TIMEFRAME_CHOICES[timeframe],
        enforce_non_overlapping=True,
    )
    account_result = run_short_account_backtest(
        run_id=run_id,
        signals=signal_result.trades,
        market_frames={
            symbol: frame.loc[:, [TIMEFRAME_CHOICES[timeframe], "open", "close"]].copy()
            for symbol, frame in market_frames.items()
        },
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
    summary["lookback_bars"] = lookback_bars
    summary["volatility_window"] = volatility_window
    summary["min_volatility_pct"] = min_volatility_pct
    summary["min_momentum_pct"] = min_momentum_pct
    summary["rebalance_interval"] = rebalance_interval
    summary["top_k"] = top_k
    summary["universe_symbols"] = len(market_frames)

    trades_path = run_dir / "trades.csv"
    orders_path = run_dir / "orders.csv"
    equity_curve_path = run_dir / "equity_curve.csv"
    summary_path = run_dir / "summary.json"
    meta_path = run_dir / "run_meta.json"
    trade_frame.to_csv(trades_path, index=False)
    orders_frame.to_csv(orders_path, index=False)
    equity_curve_frame.to_csv(equity_curve_path, index=False)

    meta = {
        "run_id": run_id,
        "strategy_key": STRATEGY_KEY,
        "strategy_label": "Inverse Short Absolute Momentum Gated Composite",
        "engine_type": "short_account",
        "timeframe": timeframe,
        "exchange": exchange_name,
        "params": {
            "lookback_bars": lookback_bars,
            "volatility_window": volatility_window,
            "hold_bars": hold_bars,
            "top_k": top_k,
            "rebalance_interval": rebalance_interval,
            "min_universe_size": min_universe_size,
            "min_volatility_pct": min_volatility_pct,
            "min_momentum_pct": min_momentum_pct,
            "quantity_step": quantity_step,
        },
        "universe": "market_data.crypto_ohlcv",
        "data_source": f"market_data.crypto_ohlcv:{exchange_name}:{timeframe}",
        "initial_capital": initial_capital,
        "position_target_pct": position_target_pct,
        "max_positions": max_positions,
        "max_gross_exposure_pct": max_gross_exposure_pct,
        "fee_rate": fee_rate,
        "slippage_per_unit": slippage_per_unit,
        "quantity_step": quantity_step,
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "source_strategy_key": "absolute-momentum-volatility-composite",
        "summary_path": str(summary_path),
        "trades_path": str(trades_path),
        "orders_path": str(orders_path),
        "equity_curve_path": str(equity_curve_path),
    }
    _write_json(summary_path, {**summary, "run_id": run_id})
    _write_json(meta_path, meta)
    return trade_frame, summary, run_dir, meta


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest a short-only inverse of the absolute-momentum-gated composite prototype")
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--symbols", help="Comma separated market symbols, for example BTC/USDT,ETH/USDT")
    parser.add_argument("--timeframe", choices=sorted(TIMEFRAME_CHOICES), default="1d")
    parser.add_argument("--lookback-bars", type=int, default=DEFAULT_LOOKBACK_BARS)
    parser.add_argument("--volatility-window", type=int, default=DEFAULT_VOLATILITY_WINDOW)
    parser.add_argument("--hold-bars", type=int, default=DEFAULT_HOLD_BARS)
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--rebalance-interval", type=int, default=DEFAULT_REBALANCE_INTERVAL)
    parser.add_argument("--min-universe-size", type=int)
    parser.add_argument("--min-volatility-pct", type=float, default=DEFAULT_MIN_VOLATILITY_PCT)
    parser.add_argument("--min-momentum-pct", type=float, default=DEFAULT_MIN_MOMENTUM_PCT)
    parser.add_argument("--initial-capital", type=float, default=100000.0)
    parser.add_argument("--position-target-pct", type=float, default=0.2)
    parser.add_argument("--max-positions", type=int, default=5)
    parser.add_argument("--max-gross-exposure-pct", type=float, default=1.0)
    parser.add_argument("--fee-rate", type=float, default=0.001)
    parser.add_argument("--slippage-per-unit", type=float, default=0.0)
    parser.add_argument("--quantity-step", type=float, default=0.0001)
    args = parser.parse_args()

    symbols = [part.strip().upper() for part in args.symbols.split(",") if part.strip()] if args.symbols else None
    trade_frame, summary, run_dir, _ = run_backtest(
        exchange_name=args.exchange,
        timeframe=args.timeframe,
        lookback_bars=args.lookback_bars,
        volatility_window=args.volatility_window,
        hold_bars=args.hold_bars,
        top_k=args.top_k,
        rebalance_interval=args.rebalance_interval,
        min_universe_size=args.min_universe_size,
        min_volatility_pct=args.min_volatility_pct,
        min_momentum_pct=args.min_momentum_pct,
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
