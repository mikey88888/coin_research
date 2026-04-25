from __future__ import annotations

import unittest

import pandas as pd

from coin_research.strategies.absolute_momentum_volatility_composite import run_absolute_momentum_volatility_composite_backtest


class AbsoluteMomentumVolatilityCompositeTests(unittest.TestCase):
    def test_rejects_negative_min_momentum_pct(self) -> None:
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
        with self.assertRaisesRegex(ValueError, "min_momentum_pct must be >= 0"):
            run_absolute_momentum_volatility_composite_backtest(
                market_frames,
                lookback_bars=2,
                volatility_window=2,
                hold_bars=2,
                top_k=1,
                rebalance_interval=2,
                min_momentum_pct=-1.0,
            )

    def test_filters_out_subthreshold_absolute_momentum(self) -> None:
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
                    "open": [100, 100.5, 101, 101.5, 102, 102.5, 103, 103.5],
                    "high": [100.5, 101, 101.5, 102, 102.5, 103, 103.5, 104],
                    "low": [99.5, 100, 100.5, 101, 101.5, 102, 102.5, 103],
                    "close": [100, 101, 102, 103, 104, 105, 106, 107],
                }
            ),
            "CCC/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 98, 96, 94, 92, 90, 88, 86],
                    "high": [101, 99, 97, 95, 93, 91, 89, 87],
                    "low": [99, 97, 95, 93, 91, 89, 87, 85],
                    "close": [100, 98, 96, 94, 92, 90, 88, 86],
                }
            ),
        }

        result = run_absolute_momentum_volatility_composite_backtest(
            market_frames,
            lookback_bars=3,
            volatility_window=3,
            hold_bars=2,
            top_k=1,
            rebalance_interval=2,
            min_volatility_pct=0.5,
            min_momentum_pct=10.0,
        )

        self.assertEqual(len(result.trades), 1)
        first_trade = result.trades[0]
        self.assertEqual(first_trade.symbol, "AAA/USDT")
        self.assertEqual(first_trade.status, "closed")
        self.assertEqual(first_trade.exit_reason, "rebalance_exit")
        self.assertGreaterEqual(first_trade.wave_drop_pct, 10.0)
        self.assertGreater(first_trade.speed5, 5.0)

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
                    "open": [10, 10.2, 10.4, 10.6, 10.8, 11.0, 11.2],
                    "high": [10.2, 10.4, 10.6, 10.8, 11.0, 11.2, 11.4],
                    "low": [9.8, 10.0, 10.2, 10.4, 10.6, 10.8, 11.0],
                    "close": [10, 10.3, 10.6, 10.9, 11.2, 11.5, 11.8],
                }
            ),
        }

        result = run_absolute_momentum_volatility_composite_backtest(
            market_frames,
            lookback_bars=2,
            volatility_window=2,
            hold_bars=2,
            top_k=1,
            rebalance_interval=3,
            min_momentum_pct=5.0,
        )

        self.assertEqual(len(result.trades), 2)
        self.assertEqual(result.trades[-1].status, "incomplete")
        self.assertIsNone(result.trades[-1].exit_date)
        self.assertIsNone(result.trades[-1].exit_price)


if __name__ == "__main__":
    unittest.main()
