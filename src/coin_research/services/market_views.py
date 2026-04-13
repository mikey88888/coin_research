from __future__ import annotations

from typing import Any

import pandas as pd

from ..data import timeframe_to_milliseconds
from ..db import load_market_summary, load_ohlcv, load_symbol_cards
from .backtest_runs import list_backtest_runs, load_backtest_run


def _safe_timestamp_label(value: Any) -> str | None:
    if value is None:
        return None
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return str(value)
    return timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")


def build_market_home_context(*, exchange_name: str = "binance", quote: str = "USDT", dsn: str | None = None) -> dict[str, Any]:
    summary = load_market_summary(exchange_name=exchange_name, quote=quote, dsn=dsn)
    cards = load_symbol_cards(exchange_name=exchange_name, quote=quote, dsn=dsn)
    latest_runs = list_backtest_runs()[:8]
    timeframe_rows = [
        {
            **row,
            "first_bar_label": _safe_timestamp_label(row.get("first_bar")),
            "last_bar_label": _safe_timestamp_label(row.get("last_bar")),
        }
        for row in summary["timeframes"]
    ]
    return {
        "page_title": "Crypto Research Dashboard",
        "exchange_name": exchange_name,
        "quote": quote,
        "market_summary": summary,
        "latest_sync_at": _safe_timestamp_label(summary.get("latest_sync_at")),
        "timeframe_rows": timeframe_rows,
        "featured_symbols": cards.sort_values(["rows_1d", "rows_4h", "symbol"], ascending=[False, False, True]).head(12).to_dict(orient="records") if not cards.empty else [],
        "latest_runs": latest_runs,
    }


def build_symbol_list_context(*, exchange_name: str = "binance", quote: str = "USDT", q: str | None = None, dsn: str | None = None) -> dict[str, Any]:
    cards = load_symbol_cards(exchange_name=exchange_name, quote=quote, dsn=dsn)
    query = (q or "").strip().upper()
    if query and not cards.empty:
        cards = cards[
            cards["symbol"].astype("string").str.upper().str.contains(query, na=False)
            | cards["base"].astype("string").str.upper().str.contains(query, na=False)
        ]
    return {
        "page_title": "币种研究",
        "exchange_name": exchange_name,
        "quote": quote,
        "query": q or "",
        "symbols": cards.to_dict(orient="records") if not cards.empty else [],
        "symbol_count": len(cards),
    }


def build_asset_detail_context(
    symbol: str,
    *,
    exchange_name: str = "binance",
    timeframe: str = "1d",
    run_id: str | None = None,
    trade_id: str | None = None,
    dsn: str | None = None,
) -> dict[str, Any]:
    frame = load_ohlcv(exchange_name=exchange_name, symbol=symbol, timeframe=timeframe, dsn=dsn)
    if frame.empty:
        raise ValueError(f"symbol not found or timeframe empty: {symbol} {timeframe}")
    frame["bar_time"] = pd.to_datetime(frame["bar_time"], errors="coerce", utc=True)
    frame = frame.sort_values("bar_time").reset_index(drop=True)
    selected_trade = None
    if run_id and trade_id:
        try:
            meta, _, trades, _, _ = load_backtest_run(run_id)
        except FileNotFoundError:
            meta = None
            trades = pd.DataFrame()
        if meta and meta.get("timeframe") == timeframe and not trades.empty:
            match = trades[
                (trades["symbol"].astype("string") == symbol)
                & (
                    (trades.get("trade_id", pd.Series(dtype="string")).astype("string") == trade_id)
                    | (trades.get("signal_id", pd.Series(dtype="string")).astype("string") == trade_id)
                )
            ]
            if not match.empty:
                selected_trade = match.iloc[0]

    view_frame = frame
    selected_trade_summary = None
    overlay = {"waveLine": [], "markers": []}
    if selected_trade is not None:
        timeframe_ms = timeframe_to_milliseconds(timeframe)
        context_bars = 12
        has_wave_points = all(
            f"p{idx}_date" in selected_trade.index and f"p{idx}_price" in selected_trade.index and pd.notna(selected_trade.get(f"p{idx}_date"))
            for idx in range(6)
        )
        start_anchor = pd.to_datetime(selected_trade["p0_date"], utc=True) if has_wave_points else pd.to_datetime(selected_trade["entry_date"], utc=True)
        start_time = start_anchor - pd.to_timedelta(context_bars * timeframe_ms, unit="ms")
        end_anchor = pd.to_datetime(selected_trade["exit_date"], utc=True) if pd.notna(selected_trade.get("exit_date")) else pd.to_datetime(selected_trade["entry_date"], utc=True)
        end_time = end_anchor + pd.to_timedelta(context_bars * timeframe_ms, unit="ms")
        narrowed = frame[(frame["bar_time"] >= start_time) & (frame["bar_time"] <= end_time)].copy()
        if not narrowed.empty:
            view_frame = narrowed.reset_index(drop=True)
        if has_wave_points:
            wave_points = []
            for idx in range(6):
                wave_points.append(
                    {
                        "time": int(pd.to_datetime(selected_trade[f"p{idx}_date"], utc=True).timestamp()),
                        "value": float(selected_trade[f"p{idx}_price"]),
                    }
                )
                overlay["markers"].append(
                    {
                        "time": int(pd.to_datetime(selected_trade[f"p{idx}_date"], utc=True).timestamp()),
                        "position": "aboveBar" if idx % 2 == 0 else "belowBar",
                        "color": "#2563eb",
                        "shape": "circle",
                        "text": f"P{idx}",
                    }
                )
            overlay["waveLine"] = wave_points
        overlay["markers"].append(
            {
                "time": int(pd.to_datetime(selected_trade["entry_date"], utc=True).timestamp()),
                "position": "belowBar",
                "color": "#16a34a",
                "shape": "arrowUp",
                "text": "BUY",
            }
        )
        if pd.notna(selected_trade.get("exit_date")):
            overlay["markers"].append(
                {
                    "time": int(pd.to_datetime(selected_trade["exit_date"], utc=True).timestamp()),
                    "position": "aboveBar",
                    "color": "#dc2626",
                    "shape": "arrowDown",
                    "text": "SELL",
                }
            )
        selected_trade_summary = {
            "run_id": run_id,
            "trade_id": trade_id,
            "entry_date": _safe_timestamp_label(selected_trade.get("entry_date")),
            "exit_date": _safe_timestamp_label(selected_trade.get("exit_date")) if pd.notna(selected_trade.get("exit_date")) else None,
            "entry_price": float(selected_trade.get("entry_price")),
            "exit_price": float(selected_trade.get("exit_price")) if pd.notna(selected_trade.get("exit_price")) else None,
            "return_pct": float(selected_trade.get("return_pct")) if pd.notna(selected_trade.get("return_pct")) else None,
            "exit_reason": selected_trade.get("exit_reason"),
        }

    latest = frame.iloc[-1]
    price_rows = view_frame.tail(300)
    chart_rows = [
        {
            "time": int(pd.Timestamp(row.bar_time).timestamp()),
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
        }
        for row in price_rows.itertuples(index=False)
    ]
    timeframe_options = ["1d", "4h", "30m", "5m"]
    return {
        "page_title": f"{symbol} {timeframe}",
        "exchange_name": exchange_name,
        "symbol": symbol,
        "timeframe": timeframe,
        "timeframe_options": timeframe_options,
        "latest_bar_time": _safe_timestamp_label(latest.bar_time),
        "latest_close": float(latest.close),
        "latest_volume": float(latest.volume) if latest.volume is not None else None,
        "row_count": len(frame),
        "first_bar_time": _safe_timestamp_label(frame.iloc[0]["bar_time"]),
        "chart_rows": chart_rows,
        "chart_overlay": overlay,
        "selected_trade": selected_trade_summary,
        "recent_rows": view_frame.tail(50).to_dict(orient="records"),
    }
