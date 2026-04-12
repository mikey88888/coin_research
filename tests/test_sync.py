from __future__ import annotations

from datetime import UTC, datetime
import unittest
from unittest.mock import patch

import pandas as pd

from coin_research.sync import (
    SYNC_POLICIES,
    compute_sync_end,
    compute_sync_start,
    resolve_top_market_cap_universe,
    sync_symbol_timeframe,
)


class SyncTests(unittest.TestCase):
    def test_resolve_top_market_cap_universe_uses_spot_usdt_markets(self) -> None:
        markets = pd.DataFrame(
            [
                {"symbol": "BTC/USDT", "base": "BTC", "quote": "USDT", "type": "spot", "spot": True, "swap": False, "future": False, "active": True},
                {"symbol": "ETH/USDT", "base": "ETH", "quote": "USDT", "type": "spot", "spot": True, "swap": False, "future": False, "active": True},
                {"symbol": "BTC/USDT:USDT", "base": "BTC", "quote": "USDT", "type": "swap", "spot": False, "swap": True, "future": False, "active": True},
            ]
        )
        with patch(
            "coin_research.sync.fetch_market_cap_page",
            side_effect=[
                pd.DataFrame(
                    [
                        {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1, "market_cap": 1},
                        {"id": "tether", "symbol": "usdt", "name": "Tether", "market_cap_rank": 2, "market_cap": 1},
                        {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 3, "market_cap": 1},
                    ]
                )
            ],
        ):
            universe = resolve_top_market_cap_universe(exchange_name="binance", markets_frame=markets, top_n=2)
        self.assertEqual(universe["market_symbol"].tolist(), ["BTC/USDT", "ETH/USDT"])

    def test_compute_sync_end_rounds_to_closed_candle(self) -> None:
        now = datetime(2026, 4, 6, 10, 31, 24, tzinfo=UTC)
        self.assertEqual(compute_sync_end(now, "5m"), datetime(2026, 4, 6, 10, 30, tzinfo=UTC))
        self.assertEqual(compute_sync_end(now, "1d"), datetime(2026, 4, 6, 0, 0, tzinfo=UTC))

    def test_compute_sync_start_respects_latest_bar(self) -> None:
        policy = next(item for item in SYNC_POLICIES if item.timeframe == "5m")
        now = datetime(2026, 4, 6, 10, 31, tzinfo=UTC)
        with patch("coin_research.sync.get_latest_bar_time", return_value=datetime(2026, 4, 6, 10, 20, tzinfo=UTC)):
            start = compute_sync_start(
                conn=object(),
                exchange_name="binance",
                symbol="BTC/USDT",
                policy=policy,
                now=now,
            )
        self.assertEqual(start, datetime(2026, 4, 6, 10, 25, tzinfo=UTC))

    def test_sync_symbol_timeframe_paginates_and_filters_open_bar(self) -> None:
        policy = next(item for item in SYNC_POLICIES if item.timeframe == "5m")
        now = datetime(2026, 4, 6, 10, 31, tzinfo=UTC)
        start_time = datetime(1970, 1, 1, 0, 0, tzinfo=UTC)
        end_time = datetime(1970, 1, 1, 0, 10, tzinfo=UTC)
        batches = [
            pd.DataFrame(
                [
                    {
                        "exchange": "binance",
                        "symbol": "BTC/USDT",
                        "timeframe": "5m",
                        "timestamp": 1_000,
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                        "volume": 1.0,
                        "datetime": pd.Timestamp(1_000, unit="ms", tz="UTC"),
                    },
                    {
                        "exchange": "binance",
                        "symbol": "BTC/USDT",
                        "timeframe": "5m",
                        "timestamp": 301_000,
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                        "volume": 1.0,
                        "datetime": pd.Timestamp(301_000, unit="ms", tz="UTC"),
                    },
                ]
            ),
            pd.DataFrame(
                [
                    {
                        "exchange": "binance",
                        "symbol": "BTC/USDT",
                        "timeframe": "5m",
                        "timestamp": 601_000,
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                        "volume": 1.0,
                        "datetime": pd.Timestamp(601_000, unit="ms", tz="UTC"),
                    }
                ]
            ),
        ]

        stored_frames: list[pd.DataFrame] = []

        def fake_upsert(_conn, frame):
            stored_frames.append(frame.copy())
            return len(frame)

        with patch("coin_research.sync.compute_sync_start", return_value=start_time), patch(
            "coin_research.sync.compute_sync_end", return_value=end_time
        ), patch("coin_research.sync.fetch_ohlcv_frame_from_exchange", side_effect=batches), patch(
            "coin_research.sync.upsert_ohlcv", side_effect=fake_upsert
        ):
            result = sync_symbol_timeframe(
                conn=object(),
                exchange=object(),
                exchange_name="binance",
                symbol="BTC/USDT",
                policy=policy,
                now=now,
                batch_limit=2,
            )
        self.assertEqual(result.fetched_rows, 2)
        self.assertEqual(result.stored_rows, 2)
        self.assertEqual(result.batches, 1)
        self.assertEqual(len(stored_frames), 1)


if __name__ == "__main__":
    unittest.main()
