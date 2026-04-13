from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
from pandas.errors import EmptyDataError

from ..config import project_root


def backtests_root(root: Path | None = None) -> Path:
    return (root or project_root()) / "reports" / "backtests"


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _safe_timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp)


def _format_timestamp(value: Any) -> str | None:
    timestamp = _safe_timestamp(value)
    if timestamp is None:
        return str(value) if value not in (None, "") else None
    return timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")


def _load_optional_csv(path_value: str | None) -> pd.DataFrame:
    if not path_value:
        return pd.DataFrame()
    path = Path(path_value)
    if not path.exists():
        return pd.DataFrame()
    try:
        return _read_csv(str(path), path.stat().st_mtime_ns).copy()
    except EmptyDataError:
        return pd.DataFrame()


def _compute_account_metrics(summary: dict[str, Any], equity_curve: pd.DataFrame, orders: pd.DataFrame) -> dict[str, Any]:
    merged = dict(summary)
    if equity_curve.empty:
        return merged

    initial_capital = _safe_float(merged.get("initial_capital"))
    if initial_capital is None and "equity" in equity_curve.columns and not equity_curve.empty:
        initial_capital = _safe_float(equity_curve.iloc[0]["equity"])
        if initial_capital is not None:
            merged["initial_capital"] = initial_capital
    ending_equity = _safe_float(merged.get("ending_equity"))
    if ending_equity is None and "equity" in equity_curve.columns and not equity_curve.empty:
        ending_equity = _safe_float(equity_curve.iloc[-1]["equity"])
        if ending_equity is not None:
            merged["ending_equity"] = ending_equity
    if "cash" in equity_curve.columns and merged.get("ending_cash") is None and not equity_curve.empty:
        ending_cash = _safe_float(equity_curve.iloc[-1]["cash"])
        if ending_cash is not None:
            merged["ending_cash"] = ending_cash

    timestamps = pd.to_datetime(equity_curve.get("timestamp"), errors="coerce", utc=True) if "timestamp" in equity_curve.columns else pd.Series(dtype="datetime64[ns]")
    timestamps = timestamps.dropna()
    if len(timestamps) >= 2:
        span_days = float((timestamps.iloc[-1] - timestamps.iloc[0]).total_seconds() / 86400.0)
        if merged.get("backtest_span_days") is None:
            merged["backtest_span_days"] = round(span_days, 4)
        if merged.get("annualized_return_pct") is None and initial_capital and ending_equity and span_days > 0:
            span_years = span_days / 365.25
            if span_years > 0:
                merged["annualized_return_pct"] = round((((ending_equity / initial_capital) ** (1.0 / span_years)) - 1.0) * 100.0, 4)

    if not orders.empty:
        fee_total = _safe_float(pd.to_numeric(orders.get("fee"), errors="coerce").fillna(0.0).sum()) if "fee" in orders.columns else 0.0
        slippage_total = _safe_float(pd.to_numeric(orders.get("slippage"), errors="coerce").fillna(0.0).sum()) if "slippage" in orders.columns else 0.0
        if merged.get("total_fees_paid") is None:
            merged["total_fees_paid"] = round(fee_total or 0.0, 4)
        if merged.get("total_slippage_paid") is None:
            merged["total_slippage_paid"] = round(slippage_total or 0.0, 4)
        if merged.get("total_trading_cost_paid") is None:
            merged["total_trading_cost_paid"] = round((fee_total or 0.0) + (slippage_total or 0.0), 4)

    return merged


@lru_cache(maxsize=64)
def _read_json(path_str: str, mtime_ns: int) -> dict[str, Any]:
    return json.loads(Path(path_str).read_text(encoding="utf-8"))


@lru_cache(maxsize=64)
def _read_csv(path_str: str, mtime_ns: int) -> pd.DataFrame:
    frame = pd.read_csv(path_str, dtype={"symbol": "string", "signal_id": "string"})
    for column in frame.columns:
        if column.endswith("_date") or column.endswith("_time") or column in {"timestamp"}:
            frame[column] = pd.to_datetime(frame[column], errors="coerce", utc=True)
    return frame


def _discover_run_meta_files(root: Path | None = None) -> list[Path]:
    return sorted(backtests_root(root).glob("*/*/run_meta.json"), reverse=True)


def list_backtest_runs(root: Path | None = None) -> list[dict[str, Any]]:
    items = []
    for meta_path in _discover_run_meta_files(root):
        payload = _read_json(str(meta_path), meta_path.stat().st_mtime_ns)
        summary_path = Path(payload["summary_path"])
        summary = _read_json(str(summary_path), summary_path.stat().st_mtime_ns) if summary_path.exists() else {}
        if payload.get("engine_type") == "account":
            summary = _compute_account_metrics(
                summary,
                _load_optional_csv(payload.get("equity_curve_path")),
                _load_optional_csv(payload.get("orders_path")),
            )
        finished_at = payload.get("finished_at")
        item = {
            "run_id": payload.get("run_id"),
            "strategy_key": payload.get("strategy_key"),
            "strategy_label": payload.get("strategy_label"),
            "engine_type": payload.get("engine_type", "signal"),
            "timeframe": payload.get("timeframe"),
            "exit_mode": payload.get("exit_mode"),
            "exchange": payload.get("exchange"),
            "finished_at": _format_timestamp(finished_at),
            "_finished_at_ts": _safe_timestamp(finished_at),
            "total_return_pct": _safe_float(summary.get("total_return_pct")),
            "annualized_return_pct": _safe_float(summary.get("annualized_return_pct")),
            "max_drawdown_pct": _safe_float(summary.get("max_drawdown_pct")),
            "signals_found": _safe_int(summary.get("signals_found")),
            "closed_trades": _safe_int(summary.get("closed_trades")),
            "win_rate": _safe_float(summary.get("win_rate")),
            "avg_return_pct": _safe_float(summary.get("avg_return_pct")),
            "detail_url": f"/research/runs/{quote_plus(str(payload.get('run_id', '')))}",
        }
        items.append(item)
    items.sort(
        key=lambda item: (
            item.get("_finished_at_ts") is not None,
            item.get("_finished_at_ts") or pd.Timestamp.min.tz_localize("UTC"),
            item.get("engine_type") == "account",
            item.get("run_id") or "",
        ),
        reverse=True,
    )
    return items


def load_backtest_run(run_id: str, root: Path | None = None) -> tuple[dict[str, Any], dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    for meta_path in _discover_run_meta_files(root):
        payload = _read_json(str(meta_path), meta_path.stat().st_mtime_ns)
        if payload.get("run_id") != run_id:
            continue
        summary_path = Path(payload["summary_path"])
        trades_path = Path(payload["trades_path"])
        summary = _read_json(str(summary_path), summary_path.stat().st_mtime_ns) if summary_path.exists() else {}
        trades = _read_csv(str(trades_path), trades_path.stat().st_mtime_ns).copy() if trades_path.exists() else pd.DataFrame()
        orders = _load_optional_csv(payload.get("orders_path"))
        equity_curve = _load_optional_csv(payload.get("equity_curve_path"))
        return payload, summary, trades, orders, equity_curve
    raise FileNotFoundError(f"run not found: {run_id}")


def leaderboard_path(root: Path | None = None) -> Path:
    return (root or project_root()) / "research" / "leaderboard.json"


def load_active_leaderboard(root: Path | None = None) -> list[dict[str, Any]]:
    path = leaderboard_path(root)
    if not path.exists():
        return []
    payload = _read_json(str(path), path.stat().st_mtime_ns)
    rows = []
    for index, item in enumerate(payload.get("active_top_results", []), start=1):
        run_id = item.get("run_id")
        rows.append(
            {
                "rank": _safe_int(item.get("rank")) or index,
                "stability": item.get("stability") or "unknown",
                "strategy_key": item.get("strategy_key"),
                "strategy_label": item.get("strategy_label") or item.get("strategy_key") or "未命名策略",
                "timeframe": item.get("timeframe"),
                "exit_mode": item.get("exit_mode"),
                "engine_type": item.get("engine_type"),
                "run_id": run_id,
                "annualized_return_pct": _safe_float(item.get("annualized_return_pct")),
                "total_return_pct": _safe_float(item.get("total_return_pct")),
                "max_drawdown_pct": _safe_float(item.get("max_drawdown_pct")),
                "closed_trades": _safe_int(item.get("closed_trades")),
                "win_rate": _safe_float(item.get("win_rate")),
                "return_drawdown_ratio": _safe_float(item.get("return_drawdown_ratio")),
                "detail_url": f"/research/runs/{quote_plus(str(run_id))}" if run_id else None,
            }
        )
    rows.sort(key=lambda item: item.get("rank") or 999999)
    return rows


def build_runs_index_context(root: Path | None = None) -> dict[str, Any]:
    runs = list_backtest_runs(root=root)
    return {
        "page_title": "回测实验台",
        "runs": runs,
        "has_runs": bool(runs),
    }


def build_leaderboard_context(root: Path | None = None) -> dict[str, Any]:
    leaderboard_rows = load_active_leaderboard(root=root)
    return {
        "page_title": "前 10 策略榜单",
        "leaderboard_rows": leaderboard_rows,
        "has_rows": bool(leaderboard_rows),
    }


def build_strategy_compare_context(strategy_key: str, root: Path | None = None) -> dict[str, Any]:
    runs = [run for run in list_backtest_runs(root=root) if run.get("strategy_key") == strategy_key]
    if not runs:
        return {
            "page_title": "策略比较",
            "strategy_key": strategy_key,
            "strategy_label": strategy_key,
            "matrix_rows": [],
            "runs": [],
            "has_runs": False,
        }

    frame = pd.DataFrame(runs)
    frame["_finished_at_ts"] = pd.to_datetime(frame["_finished_at_ts"], errors="coerce", utc=True)
    rows = []
    for (timeframe, exit_mode), group in frame.groupby(["timeframe", "exit_mode"], dropna=False):
        preferred_group = group[group["engine_type"] == "account"]
        chosen_pool = preferred_group if not preferred_group.empty else group
        chosen = chosen_pool.sort_values(["_finished_at_ts", "run_id"], ascending=[False, False], na_position="last").iloc[0]
        rows.append(
            {
                "timeframe": timeframe,
                "exit_mode": exit_mode,
                "run_count": int(len(group)),
                "latest_finished_at": chosen.get("finished_at"),
                "latest_run_id": chosen.get("run_id"),
                "latest_engine_type": chosen.get("engine_type"),
                "latest_total_return_pct": chosen.get("total_return_pct"),
                "latest_annualized_return_pct": chosen.get("annualized_return_pct"),
                "latest_max_drawdown_pct": chosen.get("max_drawdown_pct"),
                "latest_win_rate": chosen.get("win_rate"),
                "latest_closed_trades": chosen.get("closed_trades"),
                "latest_signals_found": chosen.get("signals_found"),
                "detail_url": chosen.get("detail_url"),
            }
        )
    rows.sort(key=lambda item: (item["timeframe"] or "", item["exit_mode"] or ""))
    return {
        "page_title": "策略比较",
        "strategy_key": strategy_key,
        "strategy_label": runs[0].get("strategy_label") or strategy_key,
        "matrix_rows": rows,
        "runs": runs,
        "has_runs": True,
    }


def build_run_detail_context(run_id: str, root: Path | None = None) -> dict[str, Any]:
    meta, summary, trades, orders, equity_curve = load_backtest_run(run_id, root=root)
    if meta.get("engine_type") == "account":
        summary = _compute_account_metrics(summary, equity_curve, orders)
    trade_rows = []
    if not trades.empty:
        trade_frame = trades.head(200).copy()
        for row in trade_frame.to_dict(orient="records"):
            row["detail_url"] = (
                f"/markets/crypto/{quote_plus(str(row['symbol']), safe='')}"
                f"?timeframe={quote_plus(str(meta.get('timeframe', '1d')))}"
                f"&run_id={quote_plus(str(run_id))}"
                f"&trade_id={quote_plus(str(row.get('trade_id') or row.get('signal_id') or ''))}"
            )
            trade_rows.append(row)
    equity_rows = []
    if not equity_curve.empty:
        chart_frame = equity_curve.loc[:, [column for column in ["timestamp", "equity"] if column in equity_curve.columns]].copy()
        if not chart_frame.empty:
            chart_frame["timestamp"] = pd.to_datetime(chart_frame["timestamp"], errors="coerce", utc=True)
            chart_frame["equity"] = pd.to_numeric(chart_frame["equity"], errors="coerce")
            chart_frame = chart_frame.dropna().tail(600)
            equity_rows = [
                {"time": int(item["timestamp"].timestamp()), "value": round(float(item["equity"]), 4)}
                for item in chart_frame.to_dict(orient="records")
            ]
    metric_cards = [
        ("总收益", summary.get("total_return_pct")),
        ("年化收益", summary.get("annualized_return_pct")),
        ("最大回撤", summary.get("max_drawdown_pct")),
        ("信号数", summary.get("signals_found")),
        ("闭合交易", summary.get("closed_trades")),
        ("胜率", summary.get("win_rate")),
        ("平均单笔", summary.get("avg_return_pct")),
    ]
    return {
        "page_title": f"回测详情 {run_id}",
        "run_id": run_id,
        "meta": meta,
        "summary": summary,
        "metric_cards": metric_cards,
        "trade_rows": trade_rows,
        "equity_rows": equity_rows,
    }
