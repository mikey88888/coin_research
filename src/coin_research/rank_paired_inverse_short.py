from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import shutil
from typing import Any

import pandas as pd

from .backtests import AccountConfig, run_short_account_backtest
from .config import project_root
from .db import load_ohlcv, load_tracked_symbols
from .inverse_short_signals import INVERSE_DEFINITION, build_inverse_short_signals


TIMEFRAME_CHOICES = {"1d": "bar_time", "4h": "bar_time", "30m": "bar_time", "5m": "bar_time"}
PAIRED_STRATEGY_KEY = "paired-inverse-short-ranking"
STABLE_MIN_CLOSED_TRADES = 20


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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


def _return_drawdown_ratio(summary: dict[str, Any]) -> float | None:
    existing = _safe_float(summary.get("return_drawdown_ratio"))
    if existing is not None:
        return existing
    annualized = _safe_float(summary.get("annualized_return_pct"))
    max_drawdown = _safe_float(summary.get("max_drawdown_pct"))
    if annualized is None or max_drawdown in (None, 0):
        return None
    return round(annualized / max_drawdown, 4)


def _is_forward_account_run(meta: dict[str, Any]) -> bool:
    strategy_key = str(meta.get("strategy_key") or "")
    if meta.get("engine_type") != "account":
        return False
    if meta.get("source_run_id") or meta.get("inverse_definition"):
        return False
    if strategy_key.endswith("-short") or strategy_key.endswith("-inverse-short"):
        return False
    if strategy_key == PAIRED_STRATEGY_KEY:
        return False
    return True


def _discover_meta_files(root: Path) -> list[Path]:
    return sorted((root / "reports" / "backtests").glob("*/*/run_meta.json"))


def _discover_existing_inverse(root: Path) -> dict[str, dict[str, Any]]:
    existing: dict[str, dict[str, Any]] = {}
    for meta_path in _discover_meta_files(root):
        meta = _read_json(meta_path)
        source_run_id = meta.get("source_run_id")
        if source_run_id and meta.get("inverse_definition") == INVERSE_DEFINITION:
            existing[str(source_run_id)] = meta
    return existing


def _load_market_frames(*, exchange_name: str, timeframe: str, root: Path, dsn: str | None) -> dict[str, pd.DataFrame]:
    del root
    symbols = load_tracked_symbols(exchange_name=exchange_name, timeframe=timeframe, dsn=dsn)
    frames: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        frame = load_ohlcv(exchange_name=exchange_name, symbol=symbol, timeframe=timeframe, dsn=dsn)
        if frame.empty:
            continue
        frames[symbol] = frame.loc[:, ["symbol", "bar_time", "open", "high", "low", "close", "volume", "source", "fetched_at"]].copy()
    return frames


def _run_id_for_inverse(source_run_id: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}__inverse_short__{source_run_id}"


def _inverse_run_dir(root: Path, source_strategy_key: str, run_id: str) -> Path:
    return root / "reports" / "backtests" / f"{source_strategy_key}-inverse-short" / run_id


def _write_short_artifacts(
    *,
    root: Path,
    source_meta: dict[str, Any],
    source_summary: dict[str, Any],
    run_id: str,
    account_result: Any,
) -> dict[str, Any]:
    source_strategy_key = str(source_meta["strategy_key"])
    run_dir = _inverse_run_dir(root, source_strategy_key, run_id)
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

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
    summary["source_run_id"] = source_meta.get("run_id")
    summary["source_return_drawdown_ratio"] = _return_drawdown_ratio(source_summary)
    summary["inverse_definition"] = INVERSE_DEFINITION

    trades_path = run_dir / "trades.csv"
    orders_path = run_dir / "orders.csv"
    equity_curve_path = run_dir / "equity_curve.csv"
    summary_path = run_dir / "summary.json"
    meta_path = run_dir / "run_meta.json"
    trade_frame.to_csv(trades_path, index=False)
    orders_frame.to_csv(orders_path, index=False)
    equity_curve_frame.to_csv(equity_curve_path, index=False)
    _write_json(summary_path, {**summary, "run_id": run_id})

    now = datetime.now().isoformat(timespec="seconds")
    meta = {
        "run_id": run_id,
        "strategy_key": f"{source_strategy_key}-inverse-short",
        "strategy_label": f"Inverse Short {source_meta.get('strategy_label') or source_strategy_key}",
        "engine_type": "short_account",
        "timeframe": source_meta.get("timeframe"),
        "exit_mode": source_meta.get("exit_mode"),
        "exchange": source_meta.get("exchange"),
        "params": dict(source_meta.get("params") or {}),
        "universe": source_meta.get("universe"),
        "data_source": source_meta.get("data_source"),
        "initial_capital": source_meta.get("initial_capital"),
        "position_target_pct": source_meta.get("position_target_pct"),
        "max_positions": source_meta.get("max_positions"),
        "max_gross_exposure_pct": source_meta.get("max_gross_exposure_pct"),
        "fee_rate": source_meta.get("fee_rate"),
        "slippage_per_unit": source_meta.get("slippage_per_unit"),
        "quantity_step": source_meta.get("quantity_step"),
        "source_strategy_key": source_strategy_key,
        "source_run_id": source_meta.get("run_id"),
        "inverse_definition": INVERSE_DEFINITION,
        "started_at": now,
        "finished_at": now,
        "summary_path": str(summary_path),
        "trades_path": str(trades_path),
        "orders_path": str(orders_path),
        "equity_curve_path": str(equity_curve_path),
    }
    _write_json(meta_path, meta)
    return meta


def _run_inverse_for_meta(
    *,
    root: Path,
    source_meta: dict[str, Any],
    source_summary: dict[str, Any],
    market_frames: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    timeframe = str(source_meta.get("timeframe") or "1d")
    if timeframe not in TIMEFRAME_CHOICES:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    params = dict(source_meta.get("params") or {})
    if not market_frames:
        raise RuntimeError(f"no market frames loaded for {source_meta.get('exchange') or 'binance'}:{timeframe}")
    signals = build_inverse_short_signals(
        strategy_key=str(source_meta["strategy_key"]),
        market_frames=market_frames,
        params=params,
        timeframe=timeframe,
        exit_mode=source_meta.get("exit_mode"),
        time_column=TIMEFRAME_CHOICES[timeframe],
    )
    run_id = _run_id_for_inverse(str(source_meta["run_id"]))
    account_result = run_short_account_backtest(
        run_id=run_id,
        signals=signals,
        market_frames={symbol: frame.loc[:, [TIMEFRAME_CHOICES[timeframe], "open", "close"]].copy() for symbol, frame in market_frames.items()},
        time_column=TIMEFRAME_CHOICES[timeframe],
        config=AccountConfig(
            initial_capital=float(source_meta.get("initial_capital") or 100000.0),
            position_target_pct=float(source_meta.get("position_target_pct") or 0.2),
            max_positions=int(source_meta.get("max_positions") or 5),
            max_gross_exposure_pct=float(source_meta.get("max_gross_exposure_pct") or 1.0),
            fee_rate=float(source_meta.get("fee_rate") if source_meta.get("fee_rate") is not None else 0.001),
            slippage_per_unit=float(source_meta.get("slippage_per_unit") or 0.0),
            quantity_step=float(source_meta.get("quantity_step") or params.get("quantity_step") or 0.0001),
        ),
    )
    return _write_short_artifacts(root=root, source_meta=source_meta, source_summary=source_summary, run_id=run_id, account_result=account_result)


def _load_summary(meta: dict[str, Any]) -> dict[str, Any]:
    path_value = meta.get("summary_path")
    if not path_value:
        return {}
    path = Path(path_value)
    return _read_json(path) if path.exists() else {}


def _paired_row(*, rank: int, source_meta: dict[str, Any], inverse_meta: dict[str, Any]) -> dict[str, Any] | None:
    forward_summary = _load_summary(source_meta)
    inverse_summary = _load_summary(inverse_meta)
    forward_score = _return_drawdown_ratio(forward_summary)
    inverse_score = _return_drawdown_ratio(inverse_summary)
    if forward_score is None or inverse_score is None:
        return None
    paired_score = round((forward_score + inverse_score) / 2.0, 4)
    forward_closed = _safe_int(forward_summary.get("closed_trades")) or 0
    inverse_closed = _safe_int(inverse_summary.get("closed_trades")) or 0
    return {
        "rank": rank,
        "stability": "stable" if forward_closed >= STABLE_MIN_CLOSED_TRADES and inverse_closed >= STABLE_MIN_CLOSED_TRADES else "exploratory",
        "strategy_key": source_meta.get("strategy_key"),
        "strategy_label": source_meta.get("strategy_label"),
        "timeframe": source_meta.get("timeframe"),
        "exit_mode": source_meta.get("exit_mode"),
        "engine_type": "paired_account_short",
        "run_id": source_meta.get("run_id"),
        "forward_run_id": source_meta.get("run_id"),
        "inverse_short_run_id": inverse_meta.get("run_id"),
        "annualized_return_pct": _safe_float(forward_summary.get("annualized_return_pct")),
        "total_return_pct": _safe_float(forward_summary.get("total_return_pct")),
        "max_drawdown_pct": _safe_float(forward_summary.get("max_drawdown_pct")),
        "closed_trades": forward_closed,
        "win_rate": _safe_float(forward_summary.get("win_rate")),
        "forward_return_drawdown_ratio": forward_score,
        "inverse_short_return_drawdown_ratio": inverse_score,
        "paired_return_drawdown_ratio": paired_score,
        "return_drawdown_ratio": paired_score,
        "inverse_short_annualized_return_pct": _safe_float(inverse_summary.get("annualized_return_pct")),
        "inverse_short_max_drawdown_pct": _safe_float(inverse_summary.get("max_drawdown_pct")),
        "inverse_short_closed_trades": inverse_closed,
        "source_summary": source_meta.get("summary_path"),
        "inverse_short_summary": inverse_meta.get("summary_path"),
    }


def _write_leaderboard(root: Path, rows: list[dict[str, Any]]) -> None:
    payload = {
        "version": 2,
        "policy": {
            "goal": "Rank forward strategy results paired with logical mirror short validation",
            "top_n": 10,
            "primary_metric": "(forward_return_drawdown_ratio + inverse_short_return_drawdown_ratio) / 2",
            "primary_metric_alias": "paired_return_drawdown_ratio",
            "stable_min_closed_trades": STABLE_MIN_CLOSED_TRADES,
            "inverse_definition": INVERSE_DEFINITION,
            "ranking_rules": [
                "Prefer stable pairs where both forward and inverse short closed_trades >= stable_min_closed_trades",
                "Within the same stability bucket, sort by paired_return_drawdown_ratio descending",
                "Use forward annualized_return_pct and then forward max_drawdown_pct as tie-breakers",
                "Only keep the active top 10 paired results in this file",
            ],
            "notes": [
                "Raw forward and inverse short artifacts remain under reports/backtests/",
                "return_drawdown_ratio is kept as a compatibility alias for paired_return_drawdown_ratio",
            ],
        },
        "active_top_results": rows[:10],
    }
    _write_json(root / "research" / "leaderboard.json", payload)


def run_paired_ranking(*, root: Path | None = None, force: bool = False, dsn: str | None = None, limit: int | None = None) -> dict[str, Any]:
    root_dir = root or project_root()
    existing = _discover_existing_inverse(root_dir)
    forward_metas: list[dict[str, Any]] = []
    for meta_path in _discover_meta_files(root_dir):
        meta = _read_json(meta_path)
        if _is_forward_account_run(meta):
            forward_metas.append(meta)
    forward_metas.sort(key=lambda item: str(item.get("run_id") or ""))
    if limit is not None:
        forward_metas = forward_metas[:limit]

    inverse_by_source: dict[str, dict[str, Any]] = {}
    blocked: list[dict[str, Any]] = []
    market_cache: dict[tuple[str, str], dict[str, pd.DataFrame]] = {}
    for source_meta in forward_metas:
        source_run_id = str(source_meta.get("run_id"))
        source_summary = _load_summary(source_meta)
        if source_run_id in existing and not force:
            inverse_by_source[source_run_id] = existing[source_run_id]
            continue
        try:
            exchange = str(source_meta.get("exchange") or "binance")
            timeframe = str(source_meta.get("timeframe") or "1d")
            cache_key = (exchange, timeframe)
            if cache_key not in market_cache:
                market_cache[cache_key] = _load_market_frames(exchange_name=exchange, timeframe=timeframe, root=root_dir, dsn=dsn)
            inverse_meta = _run_inverse_for_meta(
                root=root_dir,
                source_meta=source_meta,
                source_summary=source_summary,
                market_frames=market_cache[cache_key],
            )
        except Exception as exc:
            blocked.append(
                {
                    "source_run_id": source_run_id,
                    "strategy_key": source_meta.get("strategy_key"),
                    "error": str(exc),
                }
            )
            continue
        inverse_by_source[source_run_id] = inverse_meta

    rows = []
    for source_meta in forward_metas:
        source_run_id = str(source_meta.get("run_id"))
        inverse_meta = inverse_by_source.get(source_run_id)
        if not inverse_meta:
            continue
        row = _paired_row(rank=0, source_meta=source_meta, inverse_meta=inverse_meta)
        if row is not None:
            rows.append(row)
    rows.sort(
        key=lambda item: (
            item["stability"] == "stable",
            item["paired_return_drawdown_ratio"],
            item.get("annualized_return_pct") or float("-inf"),
            -1.0 * (item.get("max_drawdown_pct") or float("inf")),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    _write_leaderboard(root_dir, rows)

    report_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    report_dir = root_dir / "reports" / "backtests" / PAIRED_STRATEGY_KEY / report_id
    _write_json(report_dir / "ranking.json", {"rows": rows, "blocked": blocked})
    return {
        "forward_runs": len(forward_metas),
        "paired_rows": len(rows),
        "blocked": blocked,
        "report_dir": str(report_dir),
        "top_results": rows[:10],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run logical mirror short validation for historical account runs and rerank paired results")
    parser.add_argument("--force", action="store_true", help="Regenerate inverse short runs even when a source_run_id already has a matching inverse")
    parser.add_argument("--limit", type=int, help="Limit forward account runs processed; useful for smoke checks")
    args = parser.parse_args()
    result = run_paired_ranking(force=args.force, limit=args.limit)
    print(f"forward_runs={result['forward_runs']}")
    print(f"paired_rows={result['paired_rows']}")
    print(f"blocked={len(result['blocked'])}")
    print(f"report_dir={result['report_dir']}")
    if result["top_results"]:
        best = result["top_results"][0]
        print(f"top_rank_1={best.get('strategy_key')} {best.get('paired_return_drawdown_ratio')}")


if __name__ == "__main__":
    main()
