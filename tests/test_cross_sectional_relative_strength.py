from __future__ import annotations

import unittest

import pandas as pd

from coin_research.strategies.cross_sectional_relative_strength import run_cross_sectional_relative_strength_backtest


class CrossSectionalRelativeStrengthTests(unittest.TestCase):
    def test_rejects_invalid_universe_threshold(self) -> None:
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": pd.date_range("2026-01-01", periods=6, freq="D", tz="UTC"),
                    "open": [10, 10, 11, 12, 13, 14],
                    "high": [10, 11, 12, 13, 14, 15],
                    "low": [9, 9, 10, 11, 12, 13],
                    "close": [10, 11, 12, 13, 14, 15],
                }
            )
        }
        with self.assertRaisesRegex(ValueError, "min_universe_size must be >= top_k"):
            run_cross_sectional_relative_strength_backtest(
                market_frames,
                lookback_bars=2,
                hold_bars=2,
                top_k=2,
                rebalance_interval=2,
                min_universe_size=1,
            )

    def test_selects_top_ranked_symbol_and_closes_after_hold_period(self) -> None:
        timeline = pd.date_range("2026-01-01", periods=8, freq="D", tz="UTC")
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10, 10.5, 11, 12, 13, 14, 15, 16],
                    "high": [10.5, 11, 12, 13, 14, 15, 16, 17],
                    "low": [9.5, 10, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5],
                    "close": [10, 11, 12, 13, 14, 15, 16, 17],
                }
            ),
            "BBB/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10, 10, 10, 10.2, 10.4, 10.6, 10.8, 11.0],
                    "high": [10.2, 10.2, 10.2, 10.4, 10.6, 10.8, 11.0, 11.2],
                    "low": [9.8, 9.8, 9.8, 10.0, 10.2, 10.4, 10.6, 10.8],
                    "close": [10, 10, 10, 10.2, 10.4, 10.6, 10.8, 11.0],
                }
            ),
            "CCC/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10, 9.8, 9.6, 9.5, 9.4, 9.3, 9.2, 9.1],
                    "high": [10.1, 9.9, 9.7, 9.6, 9.5, 9.4, 9.3, 9.2],
                    "low": [9.7, 9.5, 9.4, 9.3, 9.2, 9.1, 9.0, 8.9],
                    "close": [10, 9.8, 9.6, 9.5, 9.4, 9.3, 9.2, 9.1],
                }
            ),
        }

        result = run_cross_sectional_relative_strength_backtest(
            market_frames,
            lookback_bars=2,
            hold_bars=2,
            top_k=1,
            rebalance_interval=2,
        )

        self.assertEqual(len(result.trades), 2)
        first_trade = result.trades[0]
        self.assertEqual(first_trade.symbol, "AAA/USDT")
        self.assertEqual(first_trade.status, "closed")
        self.assertEqual(first_trade.exit_reason, "rebalance_exit")
        self.assertEqual(str(first_trade.entry_date), "2026-01-04 00:00:00+00:00")
        self.assertEqual(str(first_trade.exit_date), "2026-01-06 00:00:00+00:00")
        self.assertAlmostEqual(first_trade.entry_price, 12.0)
        self.assertAlmostEqual(first_trade.exit_price or 0.0, 14.0)

    def test_marks_last_rotation_incomplete_when_future_exit_bar_missing(self) -> None:
        timeline = pd.date_range("2026-01-01", periods=7, freq="D", tz="UTC")
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10, 10.5, 11, 11.5, 12, 12.5, 13],
                    "high": [10.5, 11, 11.5, 12, 12.5, 13, 13.5],
                    "low": [9.5, 10, 10.5, 11, 11.5, 12, 12.5],
                    "close": [10, 11, 12, 13, 14, 15, 16],
                }
            ),
            "BBB/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10, 10, 10, 10.1, 10.2, 10.3, 10.4],
                    "high": [10.1, 10.1, 10.1, 10.2, 10.3, 10.4, 10.5],
                    "low": [9.9, 9.9, 9.9, 10.0, 10.1, 10.2, 10.3],
                    "close": [10, 10, 10, 10.1, 10.2, 10.3, 10.4],
                }
            ),
        }

        result = run_cross_sectional_relative_strength_backtest(
            market_frames,
            lookback_bars=2,
            hold_bars=2,
            top_k=1,
            rebalance_interval=3,
        )

        self.assertEqual(len(result.trades), 2)
        self.assertEqual(result.trades[-1].status, "incomplete")
        self.assertIsNone(result.trades[-1].exit_date)
        self.assertIsNone(result.trades[-1].exit_price)


if __name__ == "__main__":
    unittest.main()
