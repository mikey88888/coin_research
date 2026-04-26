from __future__ import annotations

import unittest

import pandas as pd

from coin_research.strategies.breadth_scaled_absolute_momentum_composite import (
    run_breadth_scaled_absolute_momentum_composite_backtest,
)


class BreadthScaledAbsoluteMomentumCompositeTests(unittest.TestCase):
    def test_rejects_out_of_range_scale_floor_ratio(self) -> None:
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": pd.date_range("2026-01-01", periods=6, freq="D", tz="UTC"),
                    "open": [10, 10.5, 11, 11.5, 12, 12.5],
                    "high": [10.5, 11, 11.5, 12, 12.5, 13],
                    "low": [9.5, 10, 10.5, 11, 11.5, 12],
                    "close": [10, 11, 12, 13, 14, 15],
                }
            )
        }
        with self.assertRaisesRegex(ValueError, r"breadth_scale_floor_ratio must be within \[0, 1\)"):
            run_breadth_scaled_absolute_momentum_composite_backtest(
                market_frames,
                lookback_bars=2,
                volatility_window=2,
                hold_bars=2,
                top_k=1,
                rebalance_interval=2,
                breadth_scale_floor_ratio=1.0,
            )

    def test_scales_position_count_with_market_breadth(self) -> None:
        timeline = pd.date_range("2026-01-01", periods=8, freq="D", tz="UTC")
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 104, 108, 112, 116, 120, 124, 128],
                    "high": [101, 105, 109, 113, 117, 121, 125, 129],
                    "low": [99, 103, 107, 111, 115, 119, 123, 127],
                    "close": [100, 106, 112, 118, 124, 130, 136, 142],
                }
            ),
            "BBB/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 101.5, 103, 104.5, 106, 107.5, 109, 110.5],
                    "high": [100.5, 102, 103.5, 105, 106.5, 108, 109.5, 111],
                    "low": [99.5, 101, 102.5, 104, 105.5, 107, 108.5, 110],
                    "close": [100, 102, 104, 106, 108, 110, 112, 114],
                }
            ),
            "CCC/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 100.4, 100.8, 101.2, 101.6, 102, 102.4, 102.8],
                    "high": [100.2, 100.6, 101.0, 101.4, 101.8, 102.2, 102.6, 103.0],
                    "low": [99.8, 100.2, 100.6, 101.0, 101.4, 101.8, 102.2, 102.6],
                    "close": [100, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5],
                }
            ),
            "DDD/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 99.5, 99, 98.5, 98, 97.5, 97, 96.5],
                    "high": [100.2, 99.7, 99.2, 98.7, 98.2, 97.7, 97.2, 96.7],
                    "low": [99.8, 99.3, 98.8, 98.3, 97.8, 97.3, 96.8, 96.3],
                    "close": [100, 99.4, 98.8, 98.2, 97.6, 97.0, 96.4, 95.8],
                }
            ),
        }

        result = run_breadth_scaled_absolute_momentum_composite_backtest(
            market_frames,
            lookback_bars=3,
            volatility_window=3,
            hold_bars=1,
            top_k=3,
            rebalance_interval=2,
            min_volatility_pct=0.5,
            min_momentum_pct=0.0,
            breadth_momentum_floor_pct=0.0,
            breadth_scale_floor_ratio=0.0,
        )

        self.assertEqual(result.evaluated_rebalance_count, 2)
        self.assertEqual(result.skipped_for_scale_zero_rebalance_count, 0)
        self.assertEqual(result.selected_rebalance_count, 2)
        self.assertEqual(len(result.trades), 4)

        first_rebalance_trades = [trade for trade in result.trades if trade.signal_confirm_date == result.trades[0].signal_confirm_date]
        self.assertEqual(len(first_rebalance_trades), 2)
        self.assertAlmostEqual(first_rebalance_trades[0].speed3, 75.0, places=3)
        self.assertEqual({trade.symbol for trade in first_rebalance_trades}, {"AAA/USDT", "BBB/USDT"})

    def test_skips_rotation_when_breadth_scale_hits_zero(self) -> None:
        timeline = pd.date_range("2026-01-01", periods=8, freq="D", tz="UTC")
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 104, 108, 112, 116, 120, 124, 128],
                    "high": [101, 105, 109, 113, 117, 121, 125, 129],
                    "low": [99, 103, 107, 111, 115, 119, 123, 127],
                    "close": [100, 106, 112, 118, 124, 130, 136, 142],
                }
            ),
            "BBB/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 99, 98, 97, 96, 95, 94, 93],
                    "high": [100.5, 99.5, 98.5, 97.5, 96.5, 95.5, 94.5, 93.5],
                    "low": [99.5, 98.5, 97.5, 96.5, 95.5, 94.5, 93.5, 92.5],
                    "close": [100, 99, 98, 97, 96, 95, 94, 93],
                }
            ),
            "CCC/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 99.5, 99, 98.5, 98, 97.5, 97, 96.5],
                    "high": [100.2, 99.7, 99.2, 98.7, 98.2, 97.7, 97.2, 96.7],
                    "low": [99.8, 99.3, 98.8, 98.3, 97.8, 97.3, 96.8, 96.3],
                    "close": [100, 99.4, 98.8, 98.2, 97.6, 97.0, 96.4, 95.8],
                }
            ),
        }

        result = run_breadth_scaled_absolute_momentum_composite_backtest(
            market_frames,
            lookback_bars=3,
            volatility_window=3,
            hold_bars=2,
            top_k=2,
            rebalance_interval=2,
            min_volatility_pct=0.5,
            min_momentum_pct=0.0,
            breadth_momentum_floor_pct=0.0,
            breadth_scale_floor_ratio=0.5,
        )

        self.assertEqual(result.evaluated_rebalance_count, 2)
        self.assertEqual(result.skipped_for_scale_zero_rebalance_count, 2)
        self.assertEqual(result.selected_rebalance_count, 0)
        self.assertEqual(result.trades, [])

    def test_marks_last_rotation_incomplete_when_future_exit_bar_missing(self) -> None:
        timeline = pd.date_range("2026-01-01", periods=7, freq="D", tz="UTC")
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10, 11, 12, 13, 14, 15, 16],
                    "high": [10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5],
                    "low": [9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5],
                    "close": [10, 11, 12, 13, 14, 15, 16],
                }
            ),
            "BBB/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10, 10.5, 11, 11.5, 12, 12.5, 13],
                    "high": [10.2, 10.7, 11.2, 11.7, 12.2, 12.7, 13.2],
                    "low": [9.8, 10.3, 10.8, 11.3, 11.8, 12.3, 12.8],
                    "close": [10, 10.6, 11.2, 11.8, 12.4, 13, 13.6],
                }
            ),
            "CCC/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10, 9.9, 9.8, 9.7, 9.6, 9.5, 9.4],
                    "high": [10.1, 10.0, 9.9, 9.8, 9.7, 9.6, 9.5],
                    "low": [9.9, 9.8, 9.7, 9.6, 9.5, 9.4, 9.3],
                    "close": [10, 9.95, 9.9, 9.85, 9.8, 9.75, 9.7],
                }
            ),
        }

        result = run_breadth_scaled_absolute_momentum_composite_backtest(
            market_frames,
            lookback_bars=2,
            volatility_window=2,
            hold_bars=2,
            top_k=2,
            rebalance_interval=3,
            min_volatility_pct=0.5,
            min_momentum_pct=0.0,
            breadth_momentum_floor_pct=0.0,
            breadth_scale_floor_ratio=0.0,
        )

        self.assertGreaterEqual(len(result.trades), 2)
        self.assertEqual(result.trades[-1].status, "incomplete")
        self.assertIsNone(result.trades[-1].exit_date)
        self.assertIsNone(result.trades[-1].exit_price)


if __name__ == "__main__":
    unittest.main()
