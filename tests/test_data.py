from __future__ import annotations

import unittest
from unittest.mock import patch

from coin_research.config import ExchangeConfig
from coin_research.data import fetch_ohlcv_frame, timeframe_to_milliseconds


class DataTests(unittest.TestCase):
    def test_timeframe_to_milliseconds_supports_standard_values(self) -> None:
        self.assertEqual(timeframe_to_milliseconds("15m"), 900_000)
        self.assertEqual(timeframe_to_milliseconds("1d"), 86_400_000)

    def test_timeframe_to_milliseconds_rejects_non_positive_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "timeframe value must be > 0"):
            timeframe_to_milliseconds("0m")

    def test_fetch_ohlcv_frame_normalizes_columns(self) -> None:
        class FakeExchange:
            def fetch_ohlcv(self, symbol: str, timeframe: str, since=None, limit: int = 500):
                return [
                    [1711929600000, 100.0, 105.0, 99.0, 103.0, 12.5],
                    [1711933200000, 103.0, 106.0, 101.0, 104.0, 10.1],
                ]

        with patch("coin_research.data.create_exchange", return_value=FakeExchange()):
            frame = fetch_ohlcv_frame(
                exchange_name="binance",
                symbol="BTC/USDT",
                timeframe="1h",
                limit=2,
                config=ExchangeConfig(exchange="binance"),
            )
        self.assertEqual(frame.iloc[0]["exchange"], "binance")
        self.assertEqual(frame.iloc[0]["symbol"], "BTC/USDT")
        self.assertEqual(frame.iloc[0]["timeframe"], "1h")
        self.assertIn("datetime", frame.columns)
        self.assertEqual(len(frame), 2)


if __name__ == "__main__":
    unittest.main()
