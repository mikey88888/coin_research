from __future__ import annotations

import unittest

import pandas as pd

from coin_research.strategies.momentum_volatility_composite import run_momentum_volatility_composite_backtest


class MomentumVolatilityCompositeTests(unittest.TestCase):
    def test_rejects_invalid_volatility_window(self) -> None:
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
        with self.assertRaisesRegex(ValueError, "volatility_window must be > 1"):
            run_momentum_volatility_composite_backtest(
                market_frames,
                lookback_bars=2,
                volatility_window=1,
                hold_bars=2,
                top_k=1,
                rebalance_interval=2,
            )

    def test_prefers_stronger_risk_adjusted_momentum(self) -> None:
        timeline = pd.date_range("2026-01-01", periods=8, freq="D", tz="UTC")
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 102, 107, 112, 117, 122, 127, 132],
                    "high": [101, 106, 111, 116, 121, 126, 131, 136],
                    "low": [99, 101, 106, 111, 116, 121, 126, 131],
                    "close": [100, 105, 110, 115, 120, 125, 130, 135],
                }
            ),
            "BBB/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 120, 95, 128, 92, 118, 88, 114],
                    "high": [101, 150, 101, 140, 110, 130, 100, 120],
                    "low": [99, 100, 85, 100, 75, 100, 70, 80],
                    "close": [100, 150, 90, 135, 80, 120, 70, 110],
                }
            ),
            "CCC/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [100, 99, 98, 97, 96, 95, 94, 93],
                    "high": [101, 100, 99, 98, 97, 96, 95, 94],
                    "low": [99, 98, 97, 96, 95, 94, 93, 92],
                    "close": [100, 99, 98, 97, 96, 95, 94, 93],
                }
            ),
        }

        result = run_momentum_volatility_composite_backtest(
            market_frames,
            lookback_bars=3,
            volatility_window=3,
            hold_bars=2,
            top_k=1,
            rebalance_interval=2,
            min_volatility_pct=0.5,
        )

        self.assertEqual(len(result.trades), 1)
        first_trade = result.trades[0]
        self.assertEqual(first_trade.symbol, "AAA/USDT")
        self.assertEqual(first_trade.status, "closed")
        self.assertEqual(first_trade.exit_reason, "rebalance_exit")
        self.assertGreater(first_trade.speed5, 1.0)
        self.assertLess(first_trade.p5_price, 5.0)

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

        result = run_momentum_volatility_composite_backtest(
            market_frames,
            lookback_bars=2,
            volatility_window=2,
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
