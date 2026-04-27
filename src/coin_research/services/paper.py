from __future__ import annotations

from datetime import UTC, datetime
import subprocess
import sys
from typing import Any

import pandas as pd

from ..config import project_root
from ..db import connect_pg, ensure_schema
from ..live.connectivity import BinanceConnectivityError, diagnose_binance_connectivity
from ..live.paper import DEFAULT_TOP_N, DEFAULT_TIMEFRAME, PaperTradingConfig, TIMEFRAME_CHOICES, is_session_stale, paper_log_path
from ..time_utils import beijing_now_label, format_beijing_ts
from ..live.store import (
    add_event,
    create_session,
    list_recent_equity,
    list_recent_events,
    list_recent_orders,
    load_active_session,
    load_latest_session,
    load_positions,
    load_session,
    mark_session_failed,
    request_stop,
)


def _format_ts(value: Any) -> str | None:
    return format_beijing_ts(value, seconds=True)


def _binance_connectivity_preflight(*, config: PaperTradingConfig) -> None:
    report = diagnose_binance_connectivity(exchange_name=config.exchange, timeout_seconds=4.0, include_ccxt=True)
    if not report["ok"]:
        raise BinanceConnectivityError(report)


def build_paper_dashboard_context(
    *,
    session_id: str | None = None,
    action_error: str | None = None,
    connectivity_report: dict[str, Any] | None = None,
    form_values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    form_values = form_values or {}
    form_timeframe = str(form_values.get("timeframe", DEFAULT_TIMEFRAME)).strip() or DEFAULT_TIMEFRAME
    form_top_n = str(form_values.get("top_n", DEFAULT_TOP_N)).strip() or str(DEFAULT_TOP_N)
    form_initial_capital = str(form_values.get("initial_capital", 100000)).strip() or "100000"
    try:
        with connect_pg() as conn:
            ensure_schema(conn)
            session = load_session(conn, session_id) if session_id else None
            if session is None:
                session = load_latest_session(conn)
            positions = load_positions(conn, str(session["session_id"])) if session else {}
            orders = list_recent_orders(conn, str(session["session_id"])) if session else []
            equity_rows = list_recent_equity(conn, str(session["session_id"])) if session else []
            events = list_recent_events(conn, str(session["session_id"])) if session else []
    except Exception as exc:
        return {
            "page_title": "模拟盘控制台",
            "session": None,
            "positions": [],
            "orders": [],
            "equity_rows": [],
            "events": [],
            "timeframe_choices": list(TIMEFRAME_CHOICES),
            "default_timeframe": DEFAULT_TIMEFRAME,
            "default_top_n": DEFAULT_TOP_N,
            "default_initial_capital": 100000,
            "form_timeframe": form_timeframe,
            "form_top_n": form_top_n,
            "form_initial_capital": form_initial_capital,
            "paper_error": str(exc),
            "action_error": action_error,
            "connectivity_report": connectivity_report,
            "connectivity_command": "uv run --no-sync coin-research diagnose-binance",
        }

    stale = is_session_stale(session, now=datetime.now(tz=UTC)) if session else False
    latest_equity = equity_rows[-1]["equity"] if equity_rows else None
    latest_drawdown = equity_rows[-1]["drawdown_pct"] if equity_rows else None
    return {
        "page_title": "模拟盘控制台",
        "session": {
            **session,
            "created_at_label": _format_ts(session.get("created_at")),
            "started_at_label": _format_ts(session.get("started_at")),
            "finished_at_label": _format_ts(session.get("finished_at")),
            "heartbeat_at_label": _format_ts(session.get("heartbeat_at")),
            "last_signal_bar_label": _format_ts(session.get("last_signal_bar")),
            "next_signal_bar_label": _format_ts(session.get("next_signal_bar")),
            "is_stale": stale,
            "latest_equity": latest_equity,
            "latest_drawdown_pct": latest_drawdown,
            "log_path": str(paper_log_path(str(session["session_id"]))),
        }
        if session
        else None,
        "positions": [
            {
                "symbol": position.symbol,
                "quantity": position.quantity,
                "entry_price": position.entry_price,
                "entry_time": position.entry_time,
                "planned_exit_time": position.planned_exit_time,
                "signal_id": position.signal_id,
            }
            for position in positions.values()
        ],
        "orders": orders,
        "equity_rows": [
            {"time": int(pd.Timestamp(row["timestamp"]).timestamp()), "value": float(row["equity"])}
            for row in equity_rows
        ],
        "events": events,
        "timeframe_choices": list(TIMEFRAME_CHOICES),
        "default_timeframe": DEFAULT_TIMEFRAME,
        "default_top_n": DEFAULT_TOP_N,
        "default_initial_capital": 100000,
        "form_timeframe": form_timeframe,
        "form_top_n": form_top_n,
        "form_initial_capital": form_initial_capital,
        "paper_error": None,
        "action_error": action_error,
        "connectivity_report": connectivity_report,
        "connectivity_command": "uv run --no-sync coin-research diagnose-binance",
    }


def start_paper_session(*, timeframe: str, top_n: int, initial_capital: float) -> str:
    config = PaperTradingConfig(timeframe=timeframe, top_n=top_n, initial_capital=initial_capital).validate()
    _binance_connectivity_preflight(config=config)
    with connect_pg() as conn:
        ensure_schema(conn)
        active = load_active_session(conn)
        if active is not None and not is_session_stale(active, now=datetime.now(tz=UTC)):
            raise RuntimeError(f"active paper session already running: {active['session_id']}")
        if active is not None and is_session_stale(active, now=datetime.now(tz=UTC)):
            mark_session_failed(conn, str(active["session_id"]), message="stale worker replaced by a new web request")
        session_id = create_session(conn, config=config)
        add_event(conn, session_id, level="info", message="paper session created from web control")
    log_path = paper_log_path(session_id)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    launch_line = (
        f"{beijing_now_label()} INFO launching paper runner"
        f" session_id={session_id} timeframe={timeframe} top_n={top_n} initial_capital={initial_capital}\n"
    )
    log_path.write_text(launch_line, encoding="utf-8")
    try:
        with log_path.open("a", encoding="utf-8") as handle:
            subprocess.Popen(
                [sys.executable, "-m", "coin_research.live.runner", "--session-id", session_id],
                cwd=str(project_root()),
                stdout=handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
    except Exception as exc:
        with connect_pg() as conn:
            ensure_schema(conn)
            add_event(conn, session_id, level="error", message="paper runner spawn failed", payload={"error": str(exc)})
            mark_session_failed(conn, session_id, message=f"spawn failed: {exc}")
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"{beijing_now_label()} ERROR spawn failed error={exc}\n")
        raise
    return session_id


def stop_paper_session() -> str:
    with connect_pg() as conn:
        ensure_schema(conn)
        active = load_active_session(conn)
        if active is None:
            raise RuntimeError("no active paper session")
        request_stop(conn, str(active["session_id"]))
        add_event(conn, str(active["session_id"]), level="info", message="stop requested from web control")
        return str(active["session_id"])
