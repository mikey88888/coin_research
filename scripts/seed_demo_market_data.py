from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta

import pandas as pd

from coin_research.db import connect_pg, refresh_dashboard_stats, upsert_markets, upsert_ohlcv


DEFAULT_SYMBOLS: list[tuple[str, str, float]] = [
    ("BTC/USDT", "BTC", 68_000.0),
    ("ETH/USDT", "ETH", 3_200.0),
    ("BNB/USDT", "BNB", 580.0),
    ("SOL/USDT", "SOL", 145.0),
    ("XRP/USDT", "XRP", 0.62),
]

TIMEFRAME_MS: dict[str, int] = {
    "1d": 86_400_000,
    "4h": 14_400_000,
    "30m": 1_800_000,
    "5m": 300_000,
}

DEFAULT_ROWS_PER_TIMEFRAME: dict[str, int] = {
    "1d": 40,
    "4h": 60,
    "30m": 80,
    "5m": 120,
}


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError(f"must be > 0, got {value}")
    return parsed


def build_demo_markets() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "base": base,
                "quote": "USDT",
                "type": "spot",
                "spot": True,
                "swap": False,
                "future": False,
                "active": True,
            }
            for symbol, base, _ in DEFAULT_SYMBOLS
        ]
    )


def build_demo_ohlcv(*, rows_scale: int = 1) -> pd.DataFrame:
    now = datetime.now(tz=UTC).replace(second=0, microsecond=0)
    bars: list[dict[str, object]] = []
    for timeframe, step_ms in TIMEFRAME_MS.items():
        row_count = DEFAULT_ROWS_PER_TIMEFRAME[timeframe] * rows_scale
        for symbol, _, base_price in DEFAULT_SYMBOLS:
            start = now - timedelta(milliseconds=step_ms * row_count)
            for idx in range(row_count):
                bar_time = start + timedelta(milliseconds=step_ms * idx)
                trend = 1 + 0.0015 * idx
                wave = ((idx % 7) - 3) * 0.0012
                close = base_price * trend * (1 + wave)
                open_ = close * 0.998
                high = close * 1.004
                low = close * 0.996
                volume = 1_000 + idx * 10
                bars.append(
                    {
                        "exchange": "binance",
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "timestamp": int(bar_time.timestamp() * 1000),
                        "datetime": bar_time,
                        "open": round(open_, 8),
                        "high": round(high, 8),
                        "low": round(low, 8),
                        "close": round(close, 8),
                        "volume": float(volume),
                    }
                )
    return pd.DataFrame(bars)


def seed_demo_market_data(*, exchange_name: str = "binance", quote: str = "USDT", rows_scale: int = 1) -> tuple[int, int]:
    markets = build_demo_markets()
    bars = build_demo_ohlcv(rows_scale=rows_scale)
    with connect_pg() as conn:
        market_count = upsert_markets(conn, markets, exchange_name=exchange_name)
        bar_count = upsert_ohlcv(conn, bars)
        refresh_dashboard_stats(conn, exchange_name=exchange_name, quote=quote)
    return market_count, bar_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed minimal local market data for the dashboard when live sync is unavailable")
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--quote", default="USDT")
    parser.add_argument("--rows-scale", type=_positive_int, default=1)
    args = parser.parse_args()

    market_count, bar_count = seed_demo_market_data(
        exchange_name=args.exchange,
        quote=args.quote,
        rows_scale=args.rows_scale,
    )
    print(f"seeded_markets={market_count}")
    print(f"seeded_bars={bar_count}")


if __name__ == "__main__":
    main()
