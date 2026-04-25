from __future__ import annotations

import unittest

import pandas as pd

from coin_research.strategies.volatility_compression_breakout import run_volatility_compression_breakout_backtest


class VolatilityCompressionBreakoutTests(unittest.TestCase):
    def test_rejects_invalid_quantile(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=10, freq="4h", tz="UTC"),
                "open": [100, 100, 100, 100, 101, 102, 103, 104, 105, 106],
                "high": [101, 101, 101, 101, 102, 103, 104, 105, 106, 107],
                "low": [99, 99, 99, 99, 100, 101, 102, 103, 104, 105],
                "close": [100, 100, 100, 100, 101, 102, 103, 104, 105, 106],
            }
        )
        with self.assertRaisesRegex(ValueError, "squeeze_quantile must be between 0 and 1"):
            run_volatility_compression_breakout_backtest(
                frame,
                symbol="BTC/USDT",
                squeeze_window=3,
                breakout_window=4,
                exit_window=2,
                squeeze_quantile=1.0,
                max_hold_bars=4,
            )

    def test_generates_closed_trade_on_squeeze_breakout_then_channel_exit(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=10, freq="4h", tz="UTC"),
                "open": [100.0, 100.1, 100.2, 100.15, 100.18, 100.2, 104.0, 103.0, 98.0, 97.5],
                "high": [100.3, 100.4, 100.5, 100.45, 100.5, 103.8, 104.2, 103.3, 98.4, 97.8],
                "low": [99.8, 99.9, 100.0, 100.0, 100.05, 100.1, 102.0, 96.0, 97.4, 96.9],
                "close": [100.0, 100.1, 100.2, 100.15, 100.18, 103.6, 103.1, 97.0, 97.5, 97.2],
            }
        )

        result = run_volatility_compression_breakout_backtest(
            frame,
            symbol="BTC/USDT",
            squeeze_window=3,
            breakout_window=4,
            exit_window=2,
            squeeze_quantile=0.5,
            max_hold_bars=4,
        )

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.status, "closed")
        self.assertEqual(trade.exit_reason, "channel_exit")
        self.assertEqual(str(trade.entry_date), "2026-01-02 00:00:00+00:00")
        self.assertEqual(str(trade.exit_date), "2026-01-02 08:00:00+00:00")
        self.assertAlmostEqual(trade.entry_price, 104.0)
        self.assertAlmostEqual(trade.exit_price or 0.0, 98.0)

    def test_generates_time_stop_when_breakout_stalls_without_channel_exit(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=11, freq="4h", tz="UTC"),
                "open": [100.0, 100.1, 100.2, 100.15, 100.18, 100.2, 104.0, 104.5, 104.7, 104.8, 104.6],
                "high": [100.3, 100.4, 100.5, 100.45, 100.5, 103.9, 104.6, 104.9, 105.0, 105.0, 104.8],
                "low": [99.8, 99.9, 100.0, 100.0, 100.05, 100.1, 103.8, 104.1, 104.2, 104.3, 104.1],
                "close": [100.0, 100.1, 100.2, 100.15, 100.18, 103.7, 104.4, 104.6, 104.7, 104.5, 104.3],
            }
        )

        result = run_volatility_compression_breakout_backtest(
            frame,
            symbol="BTC/USDT",
            squeeze_window=3,
            breakout_window=4,
            exit_window=2,
            squeeze_quantile=0.5,
            max_hold_bars=2,
        )

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.status, "closed")
        self.assertEqual(trade.exit_reason, "time_stop")
        self.assertEqual(str(trade.entry_date), "2026-01-02 00:00:00+00:00")
        self.assertEqual(str(trade.exit_date), "2026-01-02 08:00:00+00:00")
        self.assertAlmostEqual(trade.entry_price, 104.0)
        self.assertAlmostEqual(trade.exit_price or 0.0, 104.7)


if __name__ == "__main__":
    unittest.main()
