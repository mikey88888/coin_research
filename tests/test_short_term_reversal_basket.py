from __future__ import annotations

import unittest

import pandas as pd

from coin_research.strategies.short_term_reversal_basket import run_short_term_reversal_basket_backtest


class ShortTermReversalBasketTests(unittest.TestCase):
    def test_rejects_invalid_universe_threshold(self) -> None:
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": pd.date_range("2026-01-01", periods=6, freq="D", tz="UTC"),
                    "open": [10, 10.5, 11, 11.5, 12, 12.5],
                    "high": [10.5, 11, 11.5, 12, 12.5, 13],
                    "low": [9.5, 10, 10.5, 11, 11.5, 12],
                    "close": [10, 9.5, 9.0, 9.2, 9.4, 9.6],
                }
            )
        }
        with self.assertRaisesRegex(ValueError, "min_universe_size must be >= bottom_k"):
            run_short_term_reversal_basket_backtest(
                market_frames,
                lookback_bars=2,
                hold_bars=2,
                bottom_k=2,
                rebalance_interval=2,
                min_universe_size=1,
            )

    def test_selects_worst_loser_and_closes_after_hold_period(self) -> None:
        timeline = pd.date_range("2026-01-01", periods=8, freq="D", tz="UTC")
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10.0, 9.6, 9.1, 8.8, 9.1, 9.6, 10.1, 10.3],
                    "high": [10.1, 9.7, 9.2, 8.9, 9.3, 9.8, 10.2, 10.4],
                    "low": [9.8, 9.4, 8.9, 8.7, 8.9, 9.4, 9.9, 10.1],
                    "close": [10.0, 9.3, 8.7, 8.9, 9.4, 9.9, 10.2, 10.4],
                }
            ),
            "BBB/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10.0, 9.9, 9.8, 9.7, 9.8, 9.9, 10.0, 10.1],
                    "high": [10.1, 10.0, 9.9, 9.8, 9.9, 10.0, 10.1, 10.2],
                    "low": [9.9, 9.8, 9.7, 9.6, 9.7, 9.8, 9.9, 10.0],
                    "close": [10.0, 9.8, 9.6, 9.5, 9.7, 9.9, 10.0, 10.1],
                }
            ),
            "CCC/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10.0, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7],
                    "high": [10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8],
                    "low": [9.9, 10.0, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6],
                    "close": [10.0, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7],
                }
            ),
        }

        result = run_short_term_reversal_basket_backtest(
            market_frames,
            lookback_bars=2,
            hold_bars=2,
            bottom_k=1,
            rebalance_interval=2,
            min_drop_pct=1.0,
        )

        self.assertEqual(len(result.trades), 1)
        first_trade = result.trades[0]
        self.assertEqual(first_trade.symbol, "AAA/USDT")
        self.assertEqual(first_trade.status, "closed")
        self.assertEqual(first_trade.exit_reason, "rebalance_exit")
        self.assertEqual(str(first_trade.entry_date), "2026-01-04 00:00:00+00:00")
        self.assertEqual(str(first_trade.exit_date), "2026-01-06 00:00:00+00:00")
        self.assertAlmostEqual(first_trade.entry_price, 8.8)
        self.assertAlmostEqual(first_trade.exit_price or 0.0, 9.6)
        self.assertLess(first_trade.wave_drop_pct, -10.0)

    def test_min_drop_filter_skips_non_extreme_names(self) -> None:
        timeline = pd.date_range("2026-01-01", periods=6, freq="D", tz="UTC")
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10, 9.9, 9.85, 9.9, 10.0, 10.1],
                    "high": [10.1, 10.0, 9.95, 10.0, 10.1, 10.2],
                    "low": [9.9, 9.8, 9.75, 9.8, 9.9, 10.0],
                    "close": [10.0, 9.95, 9.9, 9.95, 10.0, 10.05],
                }
            ),
            "BBB/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10, 10.0, 10.0, 10.0, 10.0, 10.0],
                    "high": [10.1, 10.1, 10.1, 10.1, 10.1, 10.1],
                    "low": [9.9, 9.9, 9.9, 9.9, 9.9, 9.9],
                    "close": [10.0, 10.0, 10.0, 10.0, 10.0, 10.0],
                }
            ),
        }

        result = run_short_term_reversal_basket_backtest(
            market_frames,
            lookback_bars=2,
            hold_bars=2,
            bottom_k=1,
            rebalance_interval=2,
            min_drop_pct=2.0,
        )

        self.assertEqual(result.trades, [])

    def test_marks_last_rotation_incomplete_when_future_exit_bar_missing(self) -> None:
        timeline = pd.date_range("2026-01-01", periods=7, freq="D", tz="UTC")
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10.0, 9.7, 9.4, 9.1, 9.3, 9.6, 9.8],
                    "high": [10.1, 9.8, 9.5, 9.2, 9.4, 9.7, 9.9],
                    "low": [9.8, 9.5, 9.2, 8.9, 9.1, 9.4, 9.6],
                    "close": [10.0, 9.6, 9.1, 8.8, 9.0, 9.4, 9.7],
                }
            ),
            "BBB/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10.0, 9.95, 9.9, 9.85, 9.8, 9.75, 9.7],
                    "high": [10.1, 10.0, 9.95, 9.9, 9.85, 9.8, 9.75],
                    "low": [9.9, 9.85, 9.8, 9.75, 9.7, 9.65, 9.6],
                    "close": [10.0, 9.9, 9.8, 9.7, 9.6, 9.5, 9.4],
                }
            ),
        }

        result = run_short_term_reversal_basket_backtest(
            market_frames,
            lookback_bars=2,
            hold_bars=2,
            bottom_k=1,
            rebalance_interval=3,
            min_drop_pct=1.0,
        )

        self.assertEqual(len(result.trades), 2)
        self.assertEqual(result.trades[-1].status, "incomplete")
        self.assertIsNone(result.trades[-1].exit_date)
        self.assertIsNone(result.trades[-1].exit_price)


if __name__ == "__main__":
    unittest.main()
