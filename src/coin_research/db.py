from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import pandas as pd
import psycopg

from .config import load_project_env


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.crypto_markets (
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    base TEXT,
    quote TEXT,
    market_type TEXT,
    spot BOOLEAN,
    swap BOOLEAN,
    future BOOLEAN,
    active BOOLEAN,
    source TEXT NOT NULL DEFAULT 'ccxt:markets',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (exchange, symbol)
);

CREATE TABLE IF NOT EXISTS market_data.crypto_ohlcv (
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    bar_time TIMESTAMPTZ NOT NULL,
    open NUMERIC(38, 18) NOT NULL,
    high NUMERIC(38, 18) NOT NULL,
    low NUMERIC(38, 18) NOT NULL,
    close NUMERIC(38, 18) NOT NULL,
    volume NUMERIC(38, 18),
    source TEXT NOT NULL DEFAULT 'ccxt:ohlcv',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (exchange, symbol, timeframe, bar_time)
);

CREATE INDEX IF NOT EXISTS crypto_ohlcv_symbol_timeframe_bar_time_idx
    ON market_data.crypto_ohlcv (symbol, timeframe, bar_time);

CREATE INDEX IF NOT EXISTS crypto_ohlcv_exchange_timeframe_bar_time_idx
    ON market_data.crypto_ohlcv (exchange, timeframe, bar_time);

CREATE TABLE IF NOT EXISTS market_data.crypto_timeframe_stats (
    exchange TEXT NOT NULL,
    quote TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    symbol_count INTEGER NOT NULL,
    first_bar TIMESTAMPTZ,
    last_bar TIMESTAMPTZ,
    latest_sync_at TIMESTAMPTZ,
    PRIMARY KEY (exchange, quote, timeframe)
);

CREATE TABLE IF NOT EXISTS market_data.crypto_symbol_stats (
    exchange TEXT NOT NULL,
    quote TEXT NOT NULL,
    symbol TEXT NOT NULL,
    base TEXT,
    active BOOLEAN,
    rows_1d BIGINT NOT NULL DEFAULT 0,
    latest_1d TIMESTAMPTZ,
    rows_4h BIGINT NOT NULL DEFAULT 0,
    latest_4h TIMESTAMPTZ,
    rows_30m BIGINT NOT NULL DEFAULT 0,
    latest_30m TIMESTAMPTZ,
    rows_5m BIGINT NOT NULL DEFAULT 0,
    latest_5m TIMESTAMPTZ,
    latest_sync_at TIMESTAMPTZ,
    PRIMARY KEY (exchange, quote, symbol)
);
"""


def get_pg_dsn() -> str:
    load_project_env()
    dsn = os.environ.get("COIN_RESEARCH_PG_DSN")
    if not dsn:
        raise RuntimeError("COIN_RESEARCH_PG_DSN is not set")
    return dsn


def connect_pg(dsn: str | None = None) -> psycopg.Connection:
    conn = psycopg.connect(dsn or get_pg_dsn())
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        # The local user-space PostgreSQL package may not ship LLVM JIT libraries.
        cur.execute("SET jit = off")
    return conn


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        for statement in SCHEMA_SQL.split(";"):
            sql = statement.strip()
            if sql:
                cur.execute(sql)
    conn.commit()


def _nullable(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def _fetch_dataframe(query: str, params: list[Any] | tuple[Any, ...] | None = None, dsn: str | None = None) -> pd.DataFrame:
    with connect_pg(dsn) as conn, conn.cursor() as cur:
        cur.execute(query, params or [])
        rows = cur.fetchall()
        columns = [item.name for item in cur.description]
    return pd.DataFrame(rows, columns=columns)


def upsert_markets(
    conn: psycopg.Connection,
    df: pd.DataFrame,
    *,
    exchange_name: str,
    source: str = "ccxt:markets",
) -> int:
    if df.empty:
        return 0
    rows = [
        (
            exchange_name,
            row.symbol,
            _nullable(row.base),
            _nullable(row.quote),
            _nullable(row.type),
            _nullable(row.spot),
            _nullable(row.swap),
            _nullable(row.future),
            _nullable(row.active),
            source,
        )
        for row in df.itertuples(index=False)
    ]
    sql = """
        INSERT INTO market_data.crypto_markets (
            exchange, symbol, base, quote, market_type, spot, swap, future, active, source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (exchange, symbol) DO UPDATE
        SET base = EXCLUDED.base,
            quote = EXCLUDED.quote,
            market_type = EXCLUDED.market_type,
            spot = EXCLUDED.spot,
            swap = EXCLUDED.swap,
            future = EXCLUDED.future,
            active = EXCLUDED.active,
            source = EXCLUDED.source,
            fetched_at = NOW()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def upsert_ohlcv(conn: psycopg.Connection, df: pd.DataFrame, *, source: str = "ccxt:ohlcv") -> int:
    if df.empty:
        return 0
    rows = [
        (
            row.exchange,
            row.symbol,
            row.timeframe,
            _nullable(row.datetime),
            _nullable(row.open),
            _nullable(row.high),
            _nullable(row.low),
            _nullable(row.close),
            _nullable(row.volume),
            source,
        )
        for row in df.itertuples(index=False)
    ]
    sql = """
        INSERT INTO market_data.crypto_ohlcv (
            exchange, symbol, timeframe, bar_time, open, high, low, close, volume, source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (exchange, symbol, timeframe, bar_time) DO UPDATE
        SET open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            source = EXCLUDED.source,
            fetched_at = NOW()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def get_latest_bar_time(conn: psycopg.Connection, *, exchange_name: str, symbol: str, timeframe: str) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(bar_time)
            FROM market_data.crypto_ohlcv
            WHERE exchange = %s AND symbol = %s AND timeframe = %s
            """,
            (exchange_name, symbol, timeframe),
        )
        value = cur.fetchone()[0]
    return value


def load_markets(exchange_name: str | None = None, dsn: str | None = None) -> pd.DataFrame:
    query = """
        SELECT exchange, symbol, base, quote, market_type, spot, swap, future, active, source, fetched_at
        FROM market_data.crypto_markets
    """
    params: list[Any] = []
    if exchange_name is not None:
        query += " WHERE exchange = %s"
        params.append(exchange_name)
    query += " ORDER BY exchange, quote NULLS LAST, base NULLS LAST, symbol"
    return _fetch_dataframe(query, params=params, dsn=dsn)


def load_ohlcv(
    *,
    exchange_name: str,
    symbol: str,
    timeframe: str,
    start_time: datetime | str | None = None,
    end_time: datetime | str | None = None,
    dsn: str | None = None,
) -> pd.DataFrame:
    query = """
        SELECT exchange, symbol, timeframe, bar_time, open, high, low, close, volume, source, fetched_at
        FROM market_data.crypto_ohlcv
        WHERE exchange = %s AND symbol = %s AND timeframe = %s
    """
    params: list[Any] = [exchange_name, symbol, timeframe]
    if start_time is not None:
        query += " AND bar_time >= %s"
        params.append(start_time)
    if end_time is not None:
        query += " AND bar_time <= %s"
        params.append(end_time)
    query += " ORDER BY bar_time"
    return _fetch_dataframe(query, params=params, dsn=dsn)


def load_tracked_symbols(
    *,
    exchange_name: str,
    quote: str = "USDT",
    timeframe: str | None = None,
    dsn: str | None = None,
) -> list[str]:
    if timeframe is None:
        query = """
            SELECT symbol
            FROM market_data.crypto_symbol_stats
            WHERE exchange = %s
              AND quote = %s
            ORDER BY symbol
        """
        params: list[Any] = [exchange_name, quote]
    else:
        query = """
            SELECT DISTINCT m.symbol
            FROM market_data.crypto_markets AS m
            JOIN market_data.crypto_ohlcv AS o
              ON o.exchange = m.exchange AND o.symbol = m.symbol
            WHERE m.exchange = %s
              AND m.spot = TRUE
              AND UPPER(COALESCE(m.quote, '')) = UPPER(%s)
              AND o.timeframe = %s
            ORDER BY m.symbol
        """
        params = [exchange_name, quote, timeframe]
    frame = _fetch_dataframe(query, params=params, dsn=dsn)
    if frame.empty:
        return []
    return frame["symbol"].astype(str).tolist()


def refresh_dashboard_stats(
    conn: psycopg.Connection,
    *,
    exchange_name: str,
    quote: str = "USDT",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM market_data.crypto_timeframe_stats WHERE exchange = %s AND quote = %s",
            (exchange_name, quote),
        )
        cur.execute(
            "DELETE FROM market_data.crypto_symbol_stats WHERE exchange = %s AND quote = %s",
            (exchange_name, quote),
        )
        cur.execute(
            """
            WITH tracked AS (
                SELECT m.exchange, m.symbol, m.base, m.quote, m.active
                FROM market_data.crypto_markets AS m
                WHERE m.exchange = %s
                  AND m.spot = TRUE
                  AND UPPER(COALESCE(m.quote, '')) = UPPER(%s)
                  AND EXISTS (
                      SELECT 1
                      FROM market_data.crypto_ohlcv AS o
                      WHERE o.exchange = m.exchange
                        AND o.symbol = m.symbol
                  )
            ),
            rollup AS (
                SELECT
                    o.exchange,
                    o.symbol,
                    o.timeframe,
                    COUNT(*) AS rows,
                    MIN(o.bar_time) AS first_bar,
                    MAX(o.bar_time) AS last_bar,
                    MAX(o.fetched_at) AS latest_sync_at
                FROM market_data.crypto_ohlcv AS o
                JOIN tracked AS t
                  ON t.exchange = o.exchange AND t.symbol = o.symbol
                GROUP BY o.exchange, o.symbol, o.timeframe
            )
            INSERT INTO market_data.crypto_symbol_stats (
                exchange,
                quote,
                symbol,
                base,
                active,
                rows_1d,
                latest_1d,
                rows_4h,
                latest_4h,
                rows_30m,
                latest_30m,
                rows_5m,
                latest_5m,
                latest_sync_at
            )
            SELECT
                t.exchange,
                %s,
                t.symbol,
                t.base,
                t.active,
                COALESCE(MAX(CASE WHEN r.timeframe = '1d' THEN r.rows END), 0) AS rows_1d,
                MAX(CASE WHEN r.timeframe = '1d' THEN r.last_bar END) AS latest_1d,
                COALESCE(MAX(CASE WHEN r.timeframe = '4h' THEN r.rows END), 0) AS rows_4h,
                MAX(CASE WHEN r.timeframe = '4h' THEN r.last_bar END) AS latest_4h,
                COALESCE(MAX(CASE WHEN r.timeframe = '30m' THEN r.rows END), 0) AS rows_30m,
                MAX(CASE WHEN r.timeframe = '30m' THEN r.last_bar END) AS latest_30m,
                COALESCE(MAX(CASE WHEN r.timeframe = '5m' THEN r.rows END), 0) AS rows_5m,
                MAX(CASE WHEN r.timeframe = '5m' THEN r.last_bar END) AS latest_5m,
                MAX(r.latest_sync_at) AS latest_sync_at
            FROM tracked AS t
            LEFT JOIN rollup AS r
              ON r.exchange = t.exchange AND r.symbol = t.symbol
            GROUP BY t.exchange, t.symbol, t.base, t.active
            """,
            (exchange_name, quote, quote),
        )
        cur.execute(
            """
            WITH tracked AS (
                SELECT symbol
                FROM market_data.crypto_symbol_stats
                WHERE exchange = %s
                  AND quote = %s
            )
            INSERT INTO market_data.crypto_timeframe_stats (
                exchange,
                quote,
                timeframe,
                row_count,
                symbol_count,
                first_bar,
                last_bar,
                latest_sync_at
            )
            SELECT
                o.exchange,
                %s,
                o.timeframe,
                COUNT(*) AS row_count,
                COUNT(DISTINCT o.symbol) AS symbol_count,
                MIN(o.bar_time) AS first_bar,
                MAX(o.bar_time) AS last_bar,
                MAX(o.fetched_at) AS latest_sync_at
            FROM market_data.crypto_ohlcv AS o
            WHERE o.exchange = %s
              AND o.symbol IN (SELECT symbol FROM tracked)
            GROUP BY o.exchange, o.timeframe
            """,
            (exchange_name, quote, quote, exchange_name),
        )
    conn.commit()


def load_market_summary(*, exchange_name: str, quote: str = "USDT", dsn: str | None = None) -> dict[str, Any]:
    fast_query = """
        SELECT timeframe, row_count AS rows, symbol_count, first_bar, last_bar, latest_sync_at
        FROM market_data.crypto_timeframe_stats
        WHERE exchange = %s AND quote = %s
        ORDER BY timeframe
    """
    frame = _fetch_dataframe(fast_query, params=[exchange_name, quote], dsn=dsn)
    summary = {
        "exchange": exchange_name,
        "quote": quote,
        "tracked_symbols": 0,
        "timeframes": [],
        "latest_sync_at": None,
        "total_rows": 0,
    }
    if frame.empty:
        frame = _fetch_dataframe(
            """
            WITH tracked AS (
                SELECT exchange, symbol
                FROM market_data.crypto_markets
                WHERE exchange = %s
                  AND spot = TRUE
                  AND UPPER(COALESCE(quote, '')) = UPPER(%s)
            ),
            bars AS (
                SELECT
                    timeframe,
                    COUNT(*) AS rows,
                    COUNT(DISTINCT symbol) AS symbol_count,
                    MIN(bar_time) AS first_bar,
                    MAX(bar_time) AS last_bar,
                    MAX(fetched_at) AS latest_sync_at
                FROM market_data.crypto_ohlcv
                WHERE exchange = %s
                  AND symbol IN (SELECT symbol FROM tracked)
                GROUP BY timeframe
            )
            SELECT timeframe, rows, symbol_count, first_bar, last_bar, latest_sync_at
            FROM bars
            ORDER BY timeframe
            """,
            params=[exchange_name, quote, exchange_name],
            dsn=dsn,
        )
    if frame.empty:
        return summary
    latest_sync = None
    total_rows = 0
    rows = []
    for row in frame.itertuples(index=False):
        item = {
            "timeframe": row.timeframe,
            "rows": int(row.rows),
            "symbol_count": int(row.symbol_count),
            "first_bar": row.first_bar,
            "last_bar": row.last_bar,
            "latest_sync_at": row.latest_sync_at,
        }
        rows.append(item)
        total_rows += int(row.rows)
        summary["tracked_symbols"] = max(summary["tracked_symbols"], int(row.symbol_count))
        if latest_sync is None or (row.latest_sync_at is not None and row.latest_sync_at > latest_sync):
            latest_sync = row.latest_sync_at
    summary["timeframes"] = rows
    summary["latest_sync_at"] = latest_sync
    summary["total_rows"] = total_rows
    return summary


def load_symbol_cards(
    *,
    exchange_name: str,
    quote: str = "USDT",
    dsn: str | None = None,
) -> pd.DataFrame:
    frame = _fetch_dataframe(
        """
        SELECT
            exchange,
            quote,
            symbol,
            base,
            active,
            rows_1d,
            latest_1d,
            rows_4h,
            latest_4h,
            rows_30m,
            latest_30m,
            rows_5m,
            latest_5m,
            latest_sync_at
        FROM market_data.crypto_symbol_stats
        WHERE exchange = %s AND quote = %s
        ORDER BY symbol
        """,
        params=[exchange_name, quote],
        dsn=dsn,
    )
    if not frame.empty:
        return frame
    return _fetch_dataframe(
        """
        WITH tracked AS (
            SELECT exchange, symbol, base, quote, active
            FROM market_data.crypto_markets
            WHERE exchange = %s
              AND spot = TRUE
              AND UPPER(COALESCE(quote, '')) = UPPER(%s)
              AND EXISTS (
                  SELECT 1
                  FROM market_data.crypto_ohlcv AS o
                  WHERE o.exchange = exchange
                    AND o.symbol = symbol
              )
        ),
        rollup AS (
            SELECT
                symbol,
                timeframe,
                COUNT(*) AS rows,
                MAX(bar_time) AS last_bar,
                MAX(fetched_at) AS latest_sync_at
            FROM market_data.crypto_ohlcv
            WHERE exchange = %s
              AND symbol IN (SELECT symbol FROM tracked)
            GROUP BY symbol, timeframe
        )
        SELECT
            t.symbol,
            t.base,
            t.quote,
            t.active,
            COALESCE(MAX(CASE WHEN r.timeframe = '1d' THEN r.rows END), 0) AS rows_1d,
            MAX(CASE WHEN r.timeframe = '1d' THEN r.last_bar END) AS latest_1d,
            COALESCE(MAX(CASE WHEN r.timeframe = '4h' THEN r.rows END), 0) AS rows_4h,
            MAX(CASE WHEN r.timeframe = '4h' THEN r.last_bar END) AS latest_4h,
            COALESCE(MAX(CASE WHEN r.timeframe = '30m' THEN r.rows END), 0) AS rows_30m,
            MAX(CASE WHEN r.timeframe = '30m' THEN r.last_bar END) AS latest_30m,
            COALESCE(MAX(CASE WHEN r.timeframe = '5m' THEN r.rows END), 0) AS rows_5m,
            MAX(CASE WHEN r.timeframe = '5m' THEN r.last_bar END) AS latest_5m,
            MAX(r.latest_sync_at) AS latest_sync_at
        FROM tracked AS t
        LEFT JOIN rollup AS r ON r.symbol = t.symbol
        GROUP BY t.symbol, t.base, t.quote, t.active
        ORDER BY t.symbol
        """,
        params=[exchange_name, quote, exchange_name],
        dsn=dsn,
    )
