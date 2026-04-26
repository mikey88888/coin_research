from __future__ import annotations

import unittest

import pandas as pd

from coin_research.strategies.liquidity_screened_absolute_momentum_composite import (
    run_liquidity_screened_absolute_momentum_composite_backtest,
)


class LiquidityScreenedAbsoluteMomentumCompositeTests(unittest.TestCase):
    def test_rejects_invalid_liquidity_ratio(self) -> None:
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": pd.date_range("2026-01-01", periods=6, freq="D", tz="UTC"),
                    "open": [10, 10.5, 11, 11.5, 12, 12.5],
                    "high": [10.5, 11, 11.5, 12, 12.5, 13],
                    "low": [9.5, 10, 10.5, 11, 11.5, 12],
                    "close": [10, 11, 12, 13, 14, 15],
                    "volume": [1000, 1000, 1000, 1000, 1000, 1000],
                }
            )
        }
        with self.assertRaisesRegex(ValueError, "liquidity_universe_ratio must be within"):
            run_liquidity_screened_absolute_momentum_composite_backtest(
                market_frames,
                lookback_bars=2,
                volatility_window=2,
                liquidity_window=2,
                hold_bars=2,
                top_k=1,
                rebalance_interval=2,
                liquidity_universe_ratio=0.0,
            )

    def test_liquidity_screen_excludes_illiquid_high_momentum_symbol(self) -> None:
        timeline = pd.date_range("2026-01-01", periods=8, freq="D", tz="UTC")
        market_frames = {
            "AAA/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10, 11, 12, 13, 14, 15, 16, 17],
                    "high": [10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5],
                    "low": [9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5],
                    "close": [10, 12, 14, 16, 18, 20, 22, 24],
                    "volume": [5, 5, 5, 5, 5, 5, 5, 5],
                }
            ),
            "BBB/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [20, 21, 22, 23, 24, 25, 26, 27],
                    "high": [20.5, 21.5, 22.5, 23.5, 24.5, 25.5, 26.5, 27.5],
                    "low": [19.5, 20.5, 21.5, 22.5, 23.5, 24.5, 25.5, 26.5],
                    "close": [20, 21, 22, 24, 26, 28, 30, 32],
                    "volume": [2000, 2100, 2200, 2300, 2400, 2500, 2600, 2700],
                }
            ),
            "CCC/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [30, 30.2, 30.4, 30.6, 30.8, 31.0, 31.2, 31.4],
                    "high": [30.3, 30.5, 30.7, 30.9, 31.1, 31.3, 31.5, 31.7],
                    "low": [29.7, 29.9, 30.1, 30.3, 30.5, 30.7, 30.9, 31.1],
                    "close": [30, 30.1, 30.2, 30.3, 30.4, 30.5, 30.6, 30.7],
                    "volume": [1500, 1500, 1500, 1500, 1500, 1500, 1500, 1500],
                }
            ),
        }

        result = run_liquidity_screened_absolute_momentum_composite_backtest(
            market_frames,
            lookback_bars=3,
            volatility_window=3,
            liquidity_window=3,
            hold_bars=2,
            top_k=1,
            rebalance_interval=2,
            min_volatility_pct=0.5,
            min_momentum_pct=5.0,
            liquidity_universe_ratio=0.5,
        )

        self.assertEqual(len(result.trades), 1)
        self.assertTrue(all(trade.symbol == "BBB/USDT" for trade in result.trades))
        self.assertTrue(all(trade.speed3 > 1000 for trade in result.trades))
        self.assertGreater(result.avg_liquidity_eligible_universe_size or 0, 1.0)

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
                    "volume": [1000, 1000, 1000, 1000, 1000, 1000, 1000],
                }
            ),
            "BBB/USDT": pd.DataFrame(
                {
                    "bar_time": timeline,
                    "open": [10, 10.2, 10.4, 10.6, 10.8, 11.0, 11.2],
                    "high": [10.2, 10.4, 10.6, 10.8, 11.0, 11.2, 11.4],
                    "low": [9.8, 10.0, 10.2, 10.4, 10.6, 10.8, 11.0],
                    "close": [10, 10.3, 10.6, 10.9, 11.2, 11.5, 11.8],
                    "volume": [1200, 1200, 1200, 1200, 1200, 1200, 1200],
                }
            ),
        }

        result = run_liquidity_screened_absolute_momentum_composite_backtest(
            market_frames,
            lookback_bars=2,
            volatility_window=2,
            liquidity_window=2,
            hold_bars=2,
            top_k=1,
            rebalance_interval=3,
            min_momentum_pct=5.0,
            liquidity_universe_ratio=1.0,
        )

        self.assertEqual(len(result.trades), 2)
        self.assertEqual(result.trades[-1].status, "incomplete")
        self.assertIsNone(result.trades[-1].exit_date)
        self.assertIsNone(result.trades[-1].exit_price)


if __name__ == "__main__":
    unittest.main()
