from __future__ import annotations

import unittest

import pandas as pd

from coin_research.inverse_short_signals import build_inverse_short_signals


def _frame(symbol: str, closes: list[float]) -> pd.DataFrame:
    rows = []
    for index, close in enumerate(closes):
        rows.append(
            {
                "symbol": symbol,
                "bar_time": pd.Timestamp("2024-01-01T00:00:00Z") + pd.Timedelta(days=index),
                "open": close,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 1000.0,
                "source": "test",
                "fetched_at": pd.Timestamp("2024-01-01T00:00:00Z"),
            }
        )
    return pd.DataFrame(rows)


class InverseShortSignalTests(unittest.TestCase):
    def test_cross_sectional_relative_strength_inverse_selects_weakest_symbol(self) -> None:
        signals = build_inverse_short_signals(
            strategy_key="cross-sectional-relative-strength",
            market_frames={
                "AAA/USDT": _frame("AAA/USDT", [100, 130, 130, 130]),
                "BBB/USDT": _frame("BBB/USDT", [100, 100, 100, 100]),
                "CCC/USDT": _frame("CCC/USDT", [100, 80, 80, 80]),
            },
            params={"lookback_bars": 1, "hold_bars": 1, "top_k": 1, "rebalance_interval": 1},
            timeframe="1d",
        )

        self.assertGreaterEqual(len(signals), 1)
        self.assertEqual(signals[0].symbol, "CCC/USDT")
        self.assertLess(signals[0].wave_drop_pct, 0)

    def test_short_term_reversal_inverse_selects_strongest_symbol(self) -> None:
        signals = build_inverse_short_signals(
            strategy_key="short-term-reversal-basket",
            market_frames={
                "AAA/USDT": _frame("AAA/USDT", [100, 130, 130, 130]),
                "BBB/USDT": _frame("BBB/USDT", [100, 100, 100, 100]),
                "CCC/USDT": _frame("CCC/USDT", [100, 80, 80, 80]),
            },
            params={"lookback_bars": 1, "hold_bars": 1, "bottom_k": 1, "rebalance_interval": 1, "min_drop_pct": 0.0},
            timeframe="1d",
        )

        self.assertGreaterEqual(len(signals), 1)
        self.assertEqual(signals[0].symbol, "AAA/USDT")
        self.assertGreater(signals[0].wave_drop_pct, 0)

    def test_ema_inverse_generates_short_on_downtrend_transition(self) -> None:
        signals = build_inverse_short_signals(
            strategy_key="ema-trend-following",
            market_frames={"AAA/USDT": _frame("AAA/USDT", [100, 105, 110, 108, 104, 100, 96, 92, 88, 84, 80, 76])},
            params={"fast_window": 2, "slow_window": 3, "slope_window": 1},
            timeframe="1d",
        )

        self.assertTrue(signals)
        self.assertEqual(signals[0].symbol, "AAA/USDT")


if __name__ == "__main__":
    unittest.main()
