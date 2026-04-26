from __future__ import annotations

import json
from typing import Any

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from .paper import PaperEquityPoint, PaperOrder, PaperPosition, PaperTradingConfig, STRATEGY_KEY, generate_session_id


def create_session(conn: psycopg.Connection, *, config: PaperTradingConfig) -> str:
    session_id = generate_session_id()
    payload = config.validate().to_record()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trading_runtime.paper_sessions (
                session_id, status, exchange, quote, timeframe, strategy_key,
                initial_capital, cash, peak_equity, top_n, max_positions,
                position_target_pct, max_gross_exposure_pct, fee_rate, quantity_step,
                lookback_bars, volatility_window, hold_bars, top_k, rebalance_interval,
                min_volatility_pct, min_momentum_pct
            )
            VALUES (
                %s, 'created', %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s
            )
            """,
            (
                session_id,
                payload["exchange"],
                payload["quote"],
                payload["timeframe"],
                STRATEGY_KEY,
                payload["initial_capital"],
                payload["initial_capital"],
                payload["initial_capital"],
                payload["top_n"],
                payload["max_positions"],
                payload["position_target_pct"],
                payload["max_gross_exposure_pct"],
                payload["fee_rate"],
                payload["quantity_step"],
                payload["lookback_bars"],
                payload["volatility_window"],
                payload["hold_bars"],
                payload["top_k"],
                payload["rebalance_interval"],
                payload["min_volatility_pct"],
                payload["min_momentum_pct"],
            ),
        )
    conn.commit()
    return session_id


def load_session(conn: psycopg.Connection, session_id: str) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM trading_runtime.paper_sessions WHERE session_id = %s", (session_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def load_latest_session(conn: psycopg.Connection) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM trading_runtime.paper_sessions
            ORDER BY created_at DESC, session_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    return dict(row) if row else None


def load_active_session(conn: psycopg.Connection) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM trading_runtime.paper_sessions
            WHERE status IN ('created', 'running', 'stop_requested')
            ORDER BY created_at DESC, session_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    return dict(row) if row else None


def mark_session_running(conn: psycopg.Connection, session_id: str, *, pid: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trading_runtime.paper_sessions
            SET status = 'running',
                pid = %s,
                started_at = COALESCE(started_at, NOW()),
                heartbeat_at = NOW(),
                updated_at = NOW()
            WHERE session_id = %s
            """,
            (pid, session_id),
        )
    conn.commit()


def update_session_heartbeat(conn: psycopg.Connection, session_id: str, *, pid: int | None = None) -> None:
    with conn.cursor() as cur:
        if pid is None:
            cur.execute(
                """
                UPDATE trading_runtime.paper_sessions
                SET heartbeat_at = NOW(),
                    updated_at = NOW()
                WHERE session_id = %s
                """,
                (session_id,),
            )
        else:
            cur.execute(
                """
                UPDATE trading_runtime.paper_sessions
                SET pid = %s,
                    heartbeat_at = NOW(),
                    updated_at = NOW()
                WHERE session_id = %s
                """,
                (pid, session_id),
            )
    conn.commit()


def update_session_progress(
    conn: psycopg.Connection,
    session_id: str,
    *,
    cash: float,
    peak_equity: float,
    last_signal_bar: pd.Timestamp | None,
    next_signal_bar: pd.Timestamp | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trading_runtime.paper_sessions
            SET cash = %s,
                peak_equity = %s,
                last_signal_bar = %s,
                next_signal_bar = %s,
                updated_at = NOW()
            WHERE session_id = %s
            """,
            (cash, peak_equity, _nullable_ts(last_signal_bar), _nullable_ts(next_signal_bar), session_id),
        )
    conn.commit()


def set_session_universe(conn: psycopg.Connection, session_id: str, *, symbols: list[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trading_runtime.paper_sessions
            SET universe_symbols = %s::jsonb,
                updated_at = NOW()
            WHERE session_id = %s
            """,
            (json.dumps(symbols, ensure_ascii=False), session_id),
        )
    conn.commit()


def request_stop(conn: psycopg.Connection, session_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trading_runtime.paper_sessions
            SET stop_requested = TRUE,
                status = CASE WHEN status = 'created' THEN 'stop_requested' ELSE status END,
                updated_at = NOW()
            WHERE session_id = %s
            """,
            (session_id,),
        )
    conn.commit()


def mark_session_stopped(conn: psycopg.Connection, session_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trading_runtime.paper_sessions
            SET status = 'stopped',
                stop_requested = TRUE,
                finished_at = NOW(),
                updated_at = NOW()
            WHERE session_id = %s
            """,
            (session_id,),
        )
    conn.commit()


def mark_session_failed(conn: psycopg.Connection, session_id: str, *, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trading_runtime.paper_sessions
            SET status = 'failed',
                last_error = %s,
                finished_at = NOW(),
                updated_at = NOW()
            WHERE session_id = %s
            """,
            (message, session_id),
        )
    conn.commit()


def replace_positions(conn: psycopg.Connection, session_id: str, positions: dict[str, PaperPosition]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM trading_runtime.paper_positions WHERE session_id = %s", (session_id,))
        if positions:
            cur.executemany(
                """
                INSERT INTO trading_runtime.paper_positions (
                    session_id, symbol, signal_id, entry_time, planned_exit_time,
                    quantity, entry_price, entry_fee, entry_notional
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        session_id,
                        position.symbol,
                        position.signal_id,
                        position.entry_time,
                        position.planned_exit_time,
                        position.quantity,
                        position.entry_price,
                        position.entry_fee,
                        position.entry_notional,
                    )
                    for position in positions.values()
                ],
            )
    conn.commit()


def load_positions(conn: psycopg.Connection, session_id: str) -> dict[str, PaperPosition]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT symbol, signal_id, entry_time, planned_exit_time, quantity, entry_price, entry_fee, entry_notional
            FROM trading_runtime.paper_positions
            WHERE session_id = %s
            ORDER BY symbol
            """,
            (session_id,),
        )
        rows = cur.fetchall()
    positions: dict[str, PaperPosition] = {}
    for row in rows:
        positions[str(row["symbol"])] = PaperPosition(
            symbol=str(row["symbol"]),
            signal_id=str(row["signal_id"]),
            entry_time=pd.Timestamp(row["entry_time"]),
            planned_exit_time=pd.Timestamp(row["planned_exit_time"]),
            quantity=float(row["quantity"]),
            entry_price=float(row["entry_price"]),
            entry_fee=float(row["entry_fee"]),
            entry_notional=float(row["entry_notional"]),
        )
    return positions


def append_orders(conn: psycopg.Connection, session_id: str, orders: list[PaperOrder]) -> None:
    if not orders:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO trading_runtime.paper_orders (
                session_id, signal_id, timestamp, symbol, side, price, quantity, turnover, fee, slippage, reason
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id, signal_id, timestamp, symbol, side, reason) DO NOTHING
            """,
            [
                (
                    session_id,
                    order.signal_id,
                    order.timestamp,
                    order.symbol,
                    order.side,
                    order.price,
                    order.quantity,
                    order.turnover,
                    order.fee,
                    order.slippage,
                    order.reason,
                )
                for order in orders
            ],
        )
    conn.commit()


def append_equity_points(conn: psycopg.Connection, session_id: str, points: list[PaperEquityPoint]) -> None:
    if not points:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO trading_runtime.paper_equity_curve (
                session_id, timestamp, cash, market_value, equity, gross_exposure_pct, drawdown_pct, position_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id, timestamp) DO NOTHING
            """,
            [
                (
                    session_id,
                    point.timestamp,
                    point.cash,
                    point.market_value,
                    point.equity,
                    point.gross_exposure_pct,
                    point.drawdown_pct,
                    point.position_count,
                )
                for point in points
            ],
        )
    conn.commit()


def add_event(conn: psycopg.Connection, session_id: str, *, level: str, message: str, payload: dict[str, Any] | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trading_runtime.paper_events (session_id, level, message, payload)
            VALUES (%s, %s, %s, %s::jsonb)
            """,
            (session_id, level, message, json.dumps(payload, ensure_ascii=False) if payload is not None else None),
        )
    conn.commit()


def list_recent_orders(conn: psycopg.Connection, session_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT timestamp, symbol, side, price, quantity, turnover, fee, reason, signal_id
            FROM trading_runtime.paper_orders
            WHERE session_id = %s
            ORDER BY timestamp DESC, id DESC
            LIMIT %s
            """,
            (session_id, limit),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def list_recent_equity(conn: psycopg.Connection, session_id: str, *, limit: int = 200) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT timestamp, cash, market_value, equity, gross_exposure_pct, drawdown_pct, position_count
            FROM trading_runtime.paper_equity_curve
            WHERE session_id = %s
            ORDER BY timestamp DESC
            LIMIT %s
            """,
            (session_id, limit),
        )
        rows = cur.fetchall()
    result = [dict(row) for row in rows]
    result.reverse()
    return result


def list_recent_events(conn: psycopg.Connection, session_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT created_at, level, message, payload
            FROM trading_runtime.paper_events
            WHERE session_id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            (session_id, limit),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def _nullable_ts(value: pd.Timestamp | None) -> Any:
    if value is None:
        return None
    return pd.Timestamp(value).to_pydatetime()
