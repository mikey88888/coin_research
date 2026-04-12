from __future__ import annotations

import argparse

from .config import ExchangeConfig, load_settings
from .data import fetch_ohlcv_frame, list_markets, write_frame
from .db import connect_pg, ensure_schema, upsert_markets, upsert_ohlcv
from .sync import sync_top_market_cap_ohlcv


def _positive_int_arg(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive integer, got {value}")
    return value


def _build_config(args: argparse.Namespace) -> ExchangeConfig:
    base = load_settings()
    exchange = getattr(args, "exchange", None) or base.exchange
    return ExchangeConfig(
        exchange=exchange,
        api_key=base.api_key,
        api_secret=base.api_secret,
        enable_rate_limit=base.enable_rate_limit,
        timeout_ms=base.timeout_ms,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Crypto research toolkit")
    subparsers = parser.add_subparsers(dest="command", required=True)

    markets_parser = subparsers.add_parser("markets", help="List exchange markets")
    markets_parser.add_argument("--exchange", default=None, help="Exchange id, for example binance, okx, bybit")
    markets_parser.add_argument("--quote", default=None, help="Optional quote filter, for example USDT")
    markets_parser.add_argument("--limit", type=_positive_int_arg, default=30, help="Rows to print")
    markets_parser.add_argument("--to-db", action="store_true", help="Store markets in PostgreSQL")

    ohlcv_parser = subparsers.add_parser("ohlcv", help="Fetch OHLCV data")
    ohlcv_parser.add_argument("--exchange", default=None, help="Exchange id, for example binance")
    ohlcv_parser.add_argument("--symbol", required=True, help="Market symbol, for example BTC/USDT")
    ohlcv_parser.add_argument("--timeframe", default="1h", help="CCXT timeframe, for example 15m, 1h, 4h, 1d")
    ohlcv_parser.add_argument("--limit", type=_positive_int_arg, default=200, help="Number of candles")
    ohlcv_parser.add_argument("--since", type=int, default=None, help="Unix ms start time")
    ohlcv_parser.add_argument("--output", default=None, help="Optional CSV output path")
    ohlcv_parser.add_argument("--to-db", action="store_true", help="Store candles in PostgreSQL")

    db_init_parser = subparsers.add_parser("db-init", help="Initialize PostgreSQL schema")
    db_init_parser.add_argument("--dsn", default=None, help="Optional PostgreSQL DSN override")

    sync_parser = subparsers.add_parser("sync-top", help="Sync top market-cap coins into PostgreSQL")
    sync_parser.add_argument("--exchange", default=None, help="Exchange id, for example binance")
    sync_parser.add_argument("--quote", default="USDT", help="Quote asset filter, for example USDT")
    sync_parser.add_argument("--top", type=_positive_int_arg, default=100, help="Number of market-cap assets to sync")
    sync_parser.add_argument("--symbols-limit", type=_positive_int_arg, default=None, help="Optional symbol limit for testing")

    args = parser.parse_args()
    if args.command == "db-init":
        with connect_pg(args.dsn) as conn:
            ensure_schema(conn)
        print("schema=ready")
        return

    config = _build_config(args)

    if args.command == "markets":
        frame = list_markets(exchange_name=config.exchange, config=config)
        if args.quote:
            frame = frame[frame["quote"].astype("string") == args.quote]
        if args.to_db:
            with connect_pg() as conn:
                ensure_schema(conn)
                stored = upsert_markets(conn, frame, exchange_name=config.exchange)
            print(f"stored={stored}")
        if args.limit:
            frame = frame.head(args.limit)
        print(frame.to_string(index=False))
        return

    if args.command == "ohlcv":
        frame = fetch_ohlcv_frame(
            exchange_name=config.exchange,
            symbol=args.symbol,
            timeframe=args.timeframe,
            limit=args.limit,
            since=args.since,
            config=config,
        )
        if args.output:
            path = write_frame(frame, args.output)
            print(f"saved={path}")
        if args.to_db:
            with connect_pg() as conn:
                ensure_schema(conn)
                stored = upsert_ohlcv(conn, frame)
            print(f"stored={stored}")
        print(frame.head(10).to_string(index=False))
        print(f"rows={len(frame)}")
        return

    if args.command == "sync-top":
        with connect_pg() as conn:
            result = sync_top_market_cap_ohlcv(
                conn=conn,
                config=config,
                top_n=args.top,
                quote=args.quote,
                symbols_limit=args.symbols_limit,
                progress=print,
            )
        universe = result["universe"]
        summary = result["summary"]
        print(f"universe_size={len(universe)}")
        print(f"synced_rows={int(summary['stored_rows'].sum()) if not summary.empty else 0}")
        if not universe.empty:
            print(universe.head(10).to_string(index=False))
        return

    raise ValueError(f"unsupported command: {args.command}")
