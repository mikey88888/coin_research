from __future__ import annotations

import argparse
from datetime import UTC, datetime
import logging
import time

import pandas as pd

from ..config import ExchangeConfig
from ..data import fetch_ohlcv_frame_from_exchange, timeframe_to_milliseconds
from ..db import connect_pg, ensure_schema
from ..exchanges import create_exchange
from .paper import (
    PaperTradingConfig,
    apply_execution,
    build_market_rules,
    compute_latest_signal_time,
    execution_prices_for_time,
    paper_log_dir,
    paper_log_path,
    select_signals_for_time,
    signal_interval_delta,
    snapshot_universe_symbols,
)
from .store import (
    add_event,
    append_equity_points,
    append_orders,
    load_positions,
    load_session,
    mark_session_failed,
    mark_session_running,
    mark_session_stopped,
    replace_positions,
    set_session_universe,
    update_session_heartbeat,
    update_session_progress,
)


DEFAULT_POLL_SECONDS = 15


def _positive_int_arg(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"must be an integer, got {value!r}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive integer, got {parsed}")
    return parsed


def _configure_logging(session_id: str) -> logging.Logger:
    log_dir = paper_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = paper_log_path(session_id)
    logger = logging.getLogger(f"coin_research.paper.{session_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def _load_runtime_config(session: dict[str, object]) -> PaperTradingConfig:
    return PaperTradingConfig(
        exchange=str(session["exchange"]),
        quote=str(session["quote"]),
        timeframe=str(session["timeframe"]),
        initial_capital=float(session["initial_capital"]),
        top_n=int(session["top_n"]),
        position_target_pct=float(session["position_target_pct"]),
        max_positions=int(session["max_positions"]),
        max_gross_exposure_pct=float(session["max_gross_exposure_pct"]),
        fee_rate=float(session["fee_rate"]),
        quantity_step=float(session["quantity_step"]),
        lookback_bars=int(session["lookback_bars"]),
        volatility_window=int(session["volatility_window"]),
        hold_bars=int(session["hold_bars"]),
        top_k=int(session["top_k"]),
        rebalance_interval=int(session["rebalance_interval"]),
        min_volatility_pct=float(session["min_volatility_pct"]),
        min_momentum_pct=float(session["min_momentum_pct"]),
    ).validate()


def _fetch_market_frames(*, exchange, symbols: list[str], timeframe: str, limit: int) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        frame = fetch_ohlcv_frame_from_exchange(
            exchange=exchange,
            exchange_name="binance",
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
        )
        if not frame.empty:
            frame = frame.rename(columns={"datetime": "bar_time"})
            frames[symbol] = frame.loc[:, ["symbol", "bar_time", "open", "high", "low", "close", "volume"]].copy()
    return frames


def _load_or_snapshot_universe(*, conn, session: dict[str, object], exchange) -> list[str]:
    current = session.get("universe_symbols")
    if isinstance(current, list) and current:
        return [str(item) for item in current]
    symbols = snapshot_universe_symbols(
        exchange=exchange,
        exchange_name=str(session["exchange"]),
        top_n=int(session["top_n"]),
        quote=str(session["quote"]),
    )
    set_session_universe(conn, str(session["session_id"]), symbols=symbols)
    add_event(conn, str(session["session_id"]), level="info", message="snapshotted runtime universe", payload={"count": len(symbols)})
    return symbols


def run_session(*, session_id: str, poll_seconds: int = DEFAULT_POLL_SECONDS) -> None:
    if poll_seconds <= 0:
        raise ValueError(f"poll_seconds must be a positive integer, got {poll_seconds}")
    logger = _configure_logging(session_id)
    logger.info("runner booting session_id=%s poll_seconds=%s", session_id, poll_seconds)
    conn = None
    pid = 0
    try:
        import os

        conn = connect_pg()
        ensure_schema(conn)
        session = load_session(conn, session_id)
        if session is None:
            logger.error("paper session not found: %s", session_id)
            raise RuntimeError(f"paper session not found: {session_id}")
        config = _load_runtime_config(session)
        logger.info("loaded session config timeframe=%s top_n=%s initial_capital=%s", config.timeframe, config.top_n, config.initial_capital)
        exchange = create_exchange(ExchangeConfig(exchange=config.exchange))
        symbols = _load_or_snapshot_universe(conn=conn, session=session, exchange=exchange)
        if not symbols:
            logger.error("unable to build paper-trading universe")
            raise RuntimeError("unable to build paper-trading universe")
        logger.info("loaded runtime universe count=%s", len(symbols))
        market_rules = build_market_rules(exchange=exchange, symbols=symbols, default_step=config.quantity_step)
        mark_session_running(conn, session_id, pid=0)
        bars_limit = max(config.lookback_bars, config.volatility_window) + config.hold_bars + config.rebalance_interval + 20
        timeframe_step = signal_interval_delta(config.timeframe, bars=1)
        pid = os.getpid()
        mark_session_running(conn, session_id, pid=pid)
        logger.info("runner marked session running pid=%s", pid)
        while True:
            session = load_session(conn, session_id)
            if session is None:
                logger.error("paper session disappeared during runtime: %s", session_id)
                raise RuntimeError(f"paper session disappeared: {session_id}")
            if bool(session.get("stop_requested")):
                logger.info("stop requested; shutting down session_id=%s", session_id)
                add_event(conn, session_id, level="info", message="stop requested; shutting down runner")
                mark_session_stopped(conn, session_id)
                return

            update_session_heartbeat(conn, session_id, pid=pid)
            market_frames = _fetch_market_frames(exchange=exchange, symbols=symbols, timeframe=config.timeframe, limit=bars_limit)
            latest_signal_time = compute_latest_signal_time(datetime.now(tz=UTC), timeframe=config.timeframe)
            if latest_signal_time is None or not market_frames:
                logger.info(
                    "waiting for market data latest_signal_time=%s frames=%s",
                    latest_signal_time.isoformat() if latest_signal_time is not None else "none",
                    len(market_frames),
                )
                time.sleep(poll_seconds)
                continue

            next_signal_bar = pd.to_datetime(session.get("next_signal_bar"), errors="coerce", utc=True)
            if pd.isna(next_signal_bar):
                next_signal_bar = latest_signal_time

            positions = load_positions(conn, session_id)
            cash = float(session["cash"])
            peak_equity = float(session["peak_equity"])
            processed_any = False

            while next_signal_bar <= latest_signal_time:
                signals = select_signals_for_time(market_frames, signal_time=pd.Timestamp(next_signal_bar), config=config)
                execution_time = pd.Timestamp(next_signal_bar) + timeframe_step
                open_prices = execution_prices_for_time(market_frames, execution_time=execution_time)
                if not open_prices:
                    logger.info("execution prices unavailable execution_time=%s", execution_time.isoformat())
                    break
                result = apply_execution(
                    execution_time=execution_time,
                    session_id=session_id,
                    positions=positions,
                    signals=signals,
                    open_prices=open_prices,
                    cash=cash,
                    peak_equity=peak_equity,
                    config=config,
                    market_rules=market_rules,
                )
                positions = result.positions
                cash = result.cash
                peak_equity = result.peak_equity
                append_orders(conn, session_id, result.orders)
                append_equity_points(conn, session_id, [result.equity_point])
                replace_positions(conn, session_id, positions)
                processed_any = True
                logger.info(
                    "processed signal_bar=%s execution_time=%s signals=%s orders=%s positions=%s equity=%.4f",
                    pd.Timestamp(next_signal_bar).isoformat(),
                    execution_time.isoformat(),
                    len(signals),
                    len(result.orders),
                    len(positions),
                    result.equity_point.equity,
                )
                add_event(
                    conn,
                    session_id,
                    level="info",
                    message="processed rebalance step",
                    payload={
                        "signal_time": pd.Timestamp(next_signal_bar).isoformat(),
                        "execution_time": execution_time.isoformat(),
                        "signals": len(signals),
                        "position_count": len(positions),
                        "equity": result.equity_point.equity,
                    },
                )
                last_signal_bar = pd.Timestamp(next_signal_bar)
                next_signal_bar = pd.Timestamp(next_signal_bar) + signal_interval_delta(config.timeframe, bars=config.rebalance_interval)
                update_session_progress(
                    conn,
                    session_id,
                    cash=cash,
                    peak_equity=peak_equity,
                    last_signal_bar=last_signal_bar,
                    next_signal_bar=next_signal_bar,
                )

            if not processed_any:
                logger.info(
                    "idle heartbeat latest_signal_time=%s next_signal_bar=%s",
                    latest_signal_time.isoformat(),
                    next_signal_bar.isoformat() if not pd.isna(next_signal_bar) else "none",
                )
                update_session_heartbeat(conn, session_id, pid=pid)
            time.sleep(poll_seconds)
    except Exception as exc:
        logger.exception("runner failed session_id=%s", session_id)
        if conn is None:
            conn = connect_pg()
            ensure_schema(conn)
        add_event(conn, session_id, level="error", message="runner failed", payload={"error": str(exc)})
        mark_session_failed(conn, session_id, message=str(exc))
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the paper-trading worker for a session")
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--poll-seconds", type=_positive_int_arg, default=DEFAULT_POLL_SECONDS)
    args = parser.parse_args()
    try:
        run_session(session_id=args.session_id, poll_seconds=args.poll_seconds)
    except Exception:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
