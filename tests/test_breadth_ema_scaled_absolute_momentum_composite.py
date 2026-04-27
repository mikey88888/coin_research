from __future__ import annotations

import unittest

import pandas as pd

from coin_research.strategies.breadth_ema_scaled_absolute_momentum_composite import (
    run_breadth_ema_scaled_absolute_momentum_composite_backtest,
)


class BreadthEmaScaledAbsoluteMomentumCompositeTests(unittest.TestCase):
    def test_rejects_non_positive_breadth_ema_span(self) -> None:
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
        with self.assertRaisesRegex(ValueError, r"breadth_ema_span must be > 0"):
            run_breadth_ema_scaled_absolute_momentum_composite_backtest(
                market_frames,
                lookback_bars=2,
                volatility_window=2,
                hold_bars=2,
                top_k=1,
                rebalance_interval=2,
                breadth_scale_floor_ratio=0.2,
                breadth_ema_span=0,
            )

    def test_ema_smoothing_preserves_one_position_after_single_breadth_dip(self) -> None:
        timeline = pd.date_range("2026-01-01", periods=8, freq="D", tz="UTC")
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 110, 120, 130, 140, 150, 160, 170],
                    "high": [101, 111, 121, 131, 141, 151, 161, 171],
                    "low": [99, 109, 119, 129, 139, 149, 159, 169],
                    "close": [100, 110, 120, 130, 140, 150, 160, 170],
                }
            ),
            "BBB/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 105, 110, 106, 95, 90, 85, 80],
                    "high": [101, 106, 111, 107, 96, 91, 86, 81],
                    "low": [99, 104, 109, 105, 94, 89, 84, 79],
                    "close": [100, 105, 110, 106, 95, 90, 85, 80],
                }
            ),
            "CCC/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 103, 106, 104, 97, 94, 91, 88],
                    "high": [101, 104, 107, 105, 98, 95, 92, 89],
                    "low": [99, 102, 105, 103, 96, 93, 90, 87],
                    "close": [100, 103, 106, 104, 97, 94, 91, 88],
                }
            ),
        }

        result = run_breadth_ema_scaled_absolute_momentum_composite_backtest(
            market_frames,
            lookback_bars=2,
            volatility_window=2,
            hold_bars=1,
            top_k=2,
            rebalance_interval=2,
            min_volatility_pct=0.5,
            min_momentum_pct=0.0,
            breadth_momentum_floor_pct=0.0,
            breadth_scale_floor_ratio=0.55,
            breadth_ema_span=2,
        )

        self.assertEqual(result.evaluated_rebalance_count, 3)
        self.assertEqual(result.selected_rebalance_count, 2)
        self.assertEqual(result.skipped_for_scale_zero_rebalance_count, 1)
        self.assertEqual(len(result.trades), 3)

        second_rebalance_time = sorted({trade.signal_confirm_date for trade in result.trades})[1]
        second_rebalance_trades = [trade for trade in result.trades if trade.signal_confirm_date == second_rebalance_time]
        self.assertEqual(len(second_rebalance_trades), 1)
        self.assertEqual(second_rebalance_trades[0].symbol, "AAA/USDT")
        self.assertAlmostEqual(second_rebalance_trades[0].speed3, 55.5556, places=3)


if __name__ == "__main__":
    unittest.main()
