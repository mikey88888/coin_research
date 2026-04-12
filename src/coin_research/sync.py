from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
from typing import Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd
import psycopg

from .config import ExchangeConfig
from .data import fetch_ohlcv_frame_from_exchange, list_markets_from_exchange, timeframe_to_milliseconds
from .db import connect_pg, ensure_schema, get_latest_bar_time, refresh_dashboard_stats, upsert_markets, upsert_ohlcv
from .exchanges import create_exchange


COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
FULL_HISTORY_START = datetime(2017, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class TimeframePolicy:
    timeframe: str
    years: int = 0
    months: int = 0

    def window_start(self, *, now: datetime) -> datetime:
        if self.years == 0 and self.months == 0:
            return FULL_HISTORY_START
        shifted = shift_calendar(now, years=-self.years, months=-self.months)
        return floor_datetime_to_timeframe(shifted, self.timeframe)


SYNC_POLICIES = (
    TimeframePolicy("1d"),
    TimeframePolicy("4h"),
    TimeframePolicy("30m", years=3),
    TimeframePolicy("5m", months=6),
)


@dataclass(frozen=True)
class SyncBatchResult:
    timeframe: str
    fetched_rows: int
    stored_rows: int
    batches: int
    start_time: datetime
    end_time: datetime


def _read_json(url: str, *, timeout: int = 30) -> object:
    request = Request(
        url,
        headers={
            "accept": "application/json",
            "user-agent": "coin-research/0.1.0",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def shift_calendar(value: datetime, *, years: int = 0, months: int = 0) -> datetime:
    month_index = value.month - 1 + months + years * 12
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, _last_day_of_month(year, month))
    return value.replace(year=year, month=month, day=day)


def _last_day_of_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def floor_datetime_to_timeframe(value: datetime, timeframe: str) -> datetime:
    timeframe_ms = timeframe_to_milliseconds(timeframe)
    value_ms = int(value.timestamp() * 1000)
    return datetime.fromtimestamp((value_ms // timeframe_ms) * timeframe_ms / 1000, tz=UTC)


def _require_positive_int(*, name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer, got {value}")


def fetch_market_cap_page(*, page: int, per_page: int = 250, vs_currency: str = "usd") -> pd.DataFrame:
    query = urlencode(
        {
            "vs_currency": vs_currency,
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": "false",
            "locale": "en",
        }
    )
    payload = _read_json(f"{COINGECKO_MARKETS_URL}?{query}")
    if not isinstance(payload, list):
        raise RuntimeError("unexpected CoinGecko payload")
    frame = pd.DataFrame(payload)
    if frame.empty:
        return pd.DataFrame(columns=["id", "symbol", "name", "market_cap_rank", "market_cap"])
    desired_columns = ["id", "symbol", "name", "market_cap_rank", "market_cap"]
    available = [column for column in desired_columns if column in frame.columns]
    return frame.loc[:, available].copy()


def resolve_top_market_cap_universe(
    *,
    exchange_name: str,
    markets_frame: pd.DataFrame,
    top_n: int = 100,
    quote: str = "USDT",
    max_candidate_pages: int = 4,
) -> pd.DataFrame:
    _require_positive_int(name="top_n", value=top_n)
    _require_positive_int(name="max_candidate_pages", value=max_candidate_pages)

    spot_markets = markets_frame.copy()
    spot_markets = spot_markets[spot_markets["spot"] == True]
    spot_markets = spot_markets[spot_markets["quote"].astype("string").str.upper() == quote.upper()]
    spot_markets = spot_markets[spot_markets["base"].astype("string").str.upper() != quote.upper()]
    spot_markets = spot_markets[spot_markets["active"] != False]
    spot_markets = spot_markets.sort_values(["symbol"]).reset_index(drop=True)

    market_map: dict[str, list[dict[str, object]]] = {}
    for row in spot_markets.itertuples(index=False):
        base = str(row.base).upper()
        market_map.setdefault(base, []).append(
            {
                "symbol": row.symbol,
                "base": row.base,
                "quote": row.quote,
            }
        )

    selected: list[dict[str, object]] = []
    selected_symbols: set[str] = set()
    selected_coin_ids: set[str] = set()
    for page in range(1, max_candidate_pages + 1):
        candidates = fetch_market_cap_page(page=page)
        if candidates.empty:
            break
        for row in candidates.itertuples(index=False):
            coin_id = str(row.id)
            if coin_id in selected_coin_ids:
                continue
            matches = market_map.get(str(row.symbol).upper(), [])
            if not matches:
                continue
            match = matches[0]
            market_symbol = str(match["symbol"])
            if market_symbol in selected_symbols:
                continue
            selected.append(
                {
                    "exchange": exchange_name,
                    "market_symbol": market_symbol,
                    "base": match["base"],
                    "quote": match["quote"],
                    "coin_id": coin_id,
                    "coin_symbol": str(row.symbol).upper(),
                    "coin_name": row.name,
                    "market_cap_rank": int(row.market_cap_rank),
                    "market_cap": row.market_cap,
                }
            )
            selected_symbols.add(market_symbol)
            selected_coin_ids.add(coin_id)
            if len(selected) >= top_n:
                return pd.DataFrame(selected)
    return pd.DataFrame(selected)


def compute_sync_end(now: datetime, timeframe: str) -> datetime:
    timeframe_ms = timeframe_to_milliseconds(timeframe)
    now_ms = int(now.timestamp() * 1000)
    closed_open_ms = (now_ms // timeframe_ms) * timeframe_ms
    return datetime.fromtimestamp(closed_open_ms / 1000, tz=UTC)


def compute_sync_start(
    *,
    conn: psycopg.Connection,
    exchange_name: str,
    symbol: str,
    policy: TimeframePolicy,
    now: datetime,
) -> datetime:
    start_time = policy.window_start(now=now)
    latest = get_latest_bar_time(conn, exchange_name=exchange_name, symbol=symbol, timeframe=policy.timeframe)
    if latest is None:
        return start_time
    latest_ts = pd.Timestamp(latest)
    if latest_ts.tzinfo is None:
        latest_ts = latest_ts.tz_localize("UTC")
    latest_closed = latest_ts.to_pydatetime() + timedelta(milliseconds=timeframe_to_milliseconds(policy.timeframe))
    return max(start_time, latest_closed)


def sync_symbol_timeframe(
    *,
    conn: psycopg.Connection,
    exchange,
    exchange_name: str,
    symbol: str,
    policy: TimeframePolicy,
    now: datetime,
    progress: Callable[[str], None] | None = None,
    batch_limit: int = 1000,
) -> SyncBatchResult:
    timeframe = policy.timeframe
    start_time = compute_sync_start(
        conn=conn,
        exchange_name=exchange_name,
        symbol=symbol,
        policy=policy,
        now=now,
    )
    end_time = compute_sync_end(now, timeframe)
    if start_time >= end_time:
        return SyncBatchResult(
            timeframe=timeframe,
            fetched_rows=0,
            stored_rows=0,
            batches=0,
            start_time=start_time,
            end_time=end_time,
        )

    timeframe_ms = timeframe_to_milliseconds(timeframe)
    cursor_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)
    fetched_rows = 0
    stored_rows = 0
    batches = 0

    while cursor_ms < end_ms:
        frame = fetch_ohlcv_frame_from_exchange(
            exchange=exchange,
            exchange_name=exchange_name,
            symbol=symbol,
            timeframe=timeframe,
            limit=batch_limit,
            since=cursor_ms,
        )
        if frame.empty:
            break
        frame = frame[frame["timestamp"] < end_ms]
        frame = frame.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        if frame.empty:
            break
        stored_rows += upsert_ohlcv(conn, frame)
        fetched_rows += len(frame)
        batches += 1
        last_timestamp = int(frame.iloc[-1]["timestamp"])
        next_cursor_ms = last_timestamp + timeframe_ms
        if progress is not None:
            progress(
                f"    {timeframe} batch={batches} rows={len(frame)} last={frame.iloc[-1]['datetime']}"
            )
        if len(frame) < batch_limit or next_cursor_ms >= end_ms or next_cursor_ms <= cursor_ms:
            break
        cursor_ms = next_cursor_ms

    return SyncBatchResult(
        timeframe=timeframe,
        fetched_rows=fetched_rows,
        stored_rows=stored_rows,
        batches=batches,
        start_time=start_time,
        end_time=end_time,
    )


def sync_top_market_cap_ohlcv(
    *,
    conn: psycopg.Connection,
    config: ExchangeConfig,
    top_n: int = 100,
    quote: str = "USDT",
    symbols_limit: int | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, object]:
    active_conn = conn
    exchange = create_exchange(config)
    markets_frame = list_markets_from_exchange(exchange=exchange, exchange_name=config.exchange)
    ensure_schema(active_conn)
    upsert_markets(active_conn, markets_frame, exchange_name=config.exchange)

    universe = resolve_top_market_cap_universe(
        exchange_name=config.exchange,
        markets_frame=markets_frame,
        top_n=top_n,
        quote=quote,
    )
    if universe.empty:
        raise RuntimeError("no market-cap universe members matched exchange markets")
    if symbols_limit is not None:
        universe = universe.head(symbols_limit).reset_index(drop=True)

    now = datetime.now(tz=UTC)
    summaries: list[dict[str, object]] = []
    for index, row in enumerate(universe.itertuples(index=False), start=1):
        symbol = str(row.market_symbol)
        if progress is not None:
            progress(f"[{index}/{len(universe)}] {symbol} rank={row.market_cap_rank} coin={row.coin_name}")
        for policy in SYNC_POLICIES:
            retry_error: Exception | None = None
            for attempt in range(2):
                try:
                    result = sync_symbol_timeframe(
                        conn=active_conn,
                        exchange=exchange,
                        exchange_name=config.exchange,
                        symbol=symbol,
                        policy=policy,
                        now=now,
                        progress=progress,
                    )
                    retry_error = None
                    break
                except psycopg.Error as exc:
                    retry_error = exc
                    if progress is not None:
                        progress(
                            f"  db reconnect after {type(exc).__name__} on {symbol} {policy.timeframe}: {exc}"
                        )
                    try:
                        active_conn.close()
                    except Exception:
                        pass
                    active_conn = connect_pg()
                    ensure_schema(active_conn)
            if retry_error is not None:
                raise retry_error
            summaries.append(
                {
                    "exchange": config.exchange,
                    "symbol": symbol,
                    "coin_id": row.coin_id,
                    "coin_name": row.coin_name,
                    "market_cap_rank": row.market_cap_rank,
                    "timeframe": result.timeframe,
                    "fetched_rows": result.fetched_rows,
                    "stored_rows": result.stored_rows,
                    "batches": result.batches,
                    "window_start": result.start_time,
                    "window_end": result.end_time,
                }
            )
            if progress is not None:
                progress(
                    f"  done {policy.timeframe} fetched={result.fetched_rows} stored={result.stored_rows} "
                    f"window={result.start_time.isoformat()} -> {result.end_time.isoformat()}"
                )

    summary_frame = pd.DataFrame(summaries)
    refresh_dashboard_stats(active_conn, exchange_name=config.exchange, quote=quote)
    if active_conn is not conn:
        active_conn.close()
    return {
        "universe": universe,
        "summary": summary_frame,
    }
