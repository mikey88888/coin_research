from __future__ import annotations

import unittest

import pandas as pd

from coin_research.strategies.zscore_mean_reversion import run_zscore_mean_reversion_backtest


class ZScoreMeanReversionTests(unittest.TestCase):
    def test_rejects_invalid_thresholds(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=6, freq="4h", tz="UTC"),
                "open": [100, 101, 102, 103, 104, 105],
                "high": [101, 102, 103, 104, 105, 106],
                "low": [99, 100, 101, 102, 103, 104],
                "close": [100, 101, 102, 103, 104, 105],
            }
        )
        with self.assertRaisesRegex(ValueError, "entry_z must be greater than exit_z"):
            run_zscore_mean_reversion_backtest(frame, symbol="BTC/USDT", lookback=3, entry_z=1.0, exit_z=1.0, max_hold_bars=3)

    def test_generates_closed_trade_on_mean_reversion(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=9, freq="4h", tz="UTC"),
                "open": [100, 100, 100, 100, 90, 89, 94, 98, 99],
                "high": [101, 101, 101, 101, 91, 90, 95, 99, 100],
                "low": [99, 99, 99, 99, 88, 87, 93, 97, 98],
                "close": [100, 100, 100, 90, 89, 94, 98, 99, 100],
            }
        )

        result = run_zscore_mean_reversion_backtest(
            frame,
            symbol="BTC/USDT",
            lookback=3,
            entry_z=1.3,
            exit_z=0.2,
            max_hold_bars=4,
        )

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.status, "closed")
        self.assertEqual(trade.exit_reason, "mean_reversion")
        self.assertEqual(str(trade.entry_date), "2026-01-01 16:00:00+00:00")
        self.assertEqual(str(trade.exit_date), "2026-01-02 00:00:00+00:00")
        self.assertAlmostEqual(trade.entry_price, 90.0)
        self.assertAlmostEqual(trade.exit_price or 0.0, 94.0)

    def test_generates_time_stop_when_reversion_never_arrives(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=10, freq="4h", tz="UTC"),
                "open": [100, 100, 100, 100, 90, 89, 89, 89, 88, 88],
                "high": [101, 101, 101, 101, 91, 90, 90, 90, 89, 89],
                "low": [99, 99, 99, 99, 88, 88, 88, 88, 87, 87],
                "close": [100, 100, 100, 90, 89, 89, 89, 88, 88, 88],
            }
        )

        result = run_zscore_mean_reversion_backtest(
            frame,
            symbol="BTC/USDT",
            lookback=3,
            entry_z=1.3,
            exit_z=0.2,
            max_hold_bars=2,
        )

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.status, "closed")
        self.assertEqual(trade.exit_reason, "time_stop")
        self.assertEqual(str(trade.entry_date), "2026-01-01 16:00:00+00:00")
        self.assertEqual(str(trade.exit_date), "2026-01-02 04:00:00+00:00")
        self.assertAlmostEqual(trade.entry_price, 90.0)
        self.assertAlmostEqual(trade.exit_price or 0.0, 89.0)


if __name__ == "__main__":
    unittest.main()
