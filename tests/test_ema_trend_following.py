from __future__ import annotations

import unittest

import pandas as pd

from coin_research.strategies.ema_trend_following import run_ema_trend_following_backtest


class EmaTrendFollowingTests(unittest.TestCase):
    def test_rejects_invalid_windows(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=6, freq="4h", tz="UTC"),
                "open": [100, 101, 102, 103, 104, 105],
                "high": [101, 102, 103, 104, 105, 106],
                "low": [99, 100, 101, 102, 103, 104],
                "close": [100, 101, 102, 103, 104, 105],
            }
        )
        with self.assertRaisesRegex(ValueError, "slow_window must be greater"):
            run_ema_trend_following_backtest(frame, symbol="BTC/USDT", fast_window=3, slow_window=3, slope_window=1)

    def test_generates_closed_trade_on_trend_break(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=10, freq="4h", tz="UTC"),
                "open": [100, 99, 98, 99, 100, 103, 106, 107, 99, 97],
                "high": [101, 100, 99, 100, 101, 104, 107, 108, 100, 98],
                "low": [99, 98, 97, 98, 99, 102, 105, 106, 98, 96],
                "close": [100, 99, 98, 99, 100, 103, 106, 107, 99, 97],
            }
        )

        result = run_ema_trend_following_backtest(
            frame,
            symbol="BTC/USDT",
            fast_window=2,
            slow_window=4,
            slope_window=1,
        )

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.status, "closed")
        self.assertEqual(trade.exit_reason, "trend_break")
        self.assertEqual(str(trade.entry_date), "2026-01-01 20:00:00+00:00")
        self.assertEqual(str(trade.exit_date), "2026-01-02 12:00:00+00:00")
        self.assertAlmostEqual(trade.entry_price, 103.0)
        self.assertAlmostEqual(trade.exit_price or 0.0, 97.0)

    def test_marks_incomplete_trade_when_trend_never_breaks(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=10, freq="4h", tz="UTC"),
                "open": [100, 99, 98, 99, 100, 103, 106, 107, 109, 110],
                "high": [101, 100, 99, 100, 101, 104, 107, 108, 110, 111],
                "low": [99, 98, 97, 98, 99, 102, 105, 106, 108, 109],
                "close": [100, 99, 98, 99, 100, 103, 106, 107, 109, 110],
            }
        )

        result = run_ema_trend_following_backtest(
            frame,
            symbol="BTC/USDT",
            fast_window=2,
            slow_window=4,
            slope_window=1,
        )

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.status, "incomplete")
        self.assertIsNone(trade.exit_date)
        self.assertIsNone(trade.exit_price)


if __name__ == "__main__":
    unittest.main()
