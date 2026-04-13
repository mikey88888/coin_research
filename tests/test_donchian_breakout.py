from __future__ import annotations

import unittest

import pandas as pd

from coin_research.strategies.donchian_breakout import run_donchian_breakout_backtest


class DonchianBreakoutTests(unittest.TestCase):
    def test_rejects_invalid_windows(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=5, freq="4h", tz="UTC"),
                "open": [100, 101, 102, 103, 104],
                "high": [101, 102, 103, 104, 105],
                "low": [99, 100, 101, 102, 103],
                "close": [100, 101, 102, 103, 104],
            }
        )
        with self.assertRaisesRegex(ValueError, "exit_window must be smaller"):
            run_donchian_breakout_backtest(frame, symbol="BTC/USDT", breakout_window=10, exit_window=10)

    def test_generates_closed_trade_on_breakout_then_channel_exit(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=8, freq="4h", tz="UTC"),
                "open": [100, 101, 102, 103, 104, 110, 108, 97],
                "high": [101, 102, 103, 104, 110, 111, 109, 98],
                "low": [99, 100, 101, 102, 103, 109, 95, 96],
                "close": [100, 101, 102, 109, 110, 108, 96, 97],
            }
        )

        result = run_donchian_breakout_backtest(
            frame,
            symbol="BTC/USDT",
            breakout_window=3,
            exit_window=2,
        )

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.status, "closed")
        self.assertEqual(str(trade.entry_date), "2026-01-01 16:00:00+00:00")
        self.assertEqual(str(trade.exit_date), "2026-01-02 04:00:00+00:00")
        self.assertEqual(trade.exit_reason, "channel_exit")
        self.assertAlmostEqual(trade.entry_price, 104.0)
        self.assertAlmostEqual(trade.exit_price or 0.0, 97.0)

    def test_marks_open_trade_when_no_channel_exit_arrives(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=7, freq="4h", tz="UTC"),
                "open": [100, 101, 102, 103, 104, 106, 108],
                "high": [101, 102, 103, 108, 109, 110, 111],
                "low": [99, 100, 101, 102, 103, 105, 107],
                "close": [100, 101, 102, 107, 108, 109, 110],
            }
        )

        result = run_donchian_breakout_backtest(
            frame,
            symbol="BTC/USDT",
            breakout_window=3,
            exit_window=2,
        )

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.status, "incomplete")
        self.assertIsNone(trade.exit_date)
        self.assertIsNone(trade.exit_price)


if __name__ == "__main__":
    unittest.main()
