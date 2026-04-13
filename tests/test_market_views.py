from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from coin_research.services.market_views import build_asset_detail_context


class MarketViewsTests(unittest.TestCase):
    def test_asset_detail_handles_non_wave_trade_overlay(self) -> None:
        frame = pd.DataFrame(
            {
                "bar_time": pd.date_range("2026-01-01", periods=12, freq="4h", tz="UTC"),
                "open": [100 + idx for idx in range(12)],
                "high": [101 + idx for idx in range(12)],
                "low": [99 + idx for idx in range(12)],
                "close": [100.5 + idx for idx in range(12)],
                "volume": [10_000 + idx for idx in range(12)],
            }
        )
        trades = pd.DataFrame(
            [
                {
                    "symbol": "BTC/USDT",
                    "trade_id": "trade-1",
                    "entry_date": pd.Timestamp("2026-01-01 08:00:00", tz="UTC"),
                    "exit_date": pd.Timestamp("2026-01-02 00:00:00", tz="UTC"),
                    "entry_price": 102.0,
                    "exit_price": 107.0,
                    "return_pct": 4.9,
                    "exit_reason": "trend_break",
                }
            ]
        )

        with patch("coin_research.services.market_views.load_ohlcv", return_value=frame), patch(
            "coin_research.services.market_views.load_backtest_run",
            return_value=({"timeframe": "4h"}, {}, trades, pd.DataFrame(), pd.DataFrame()),
        ):
            context = build_asset_detail_context("BTC/USDT", timeframe="4h", run_id="ema-run", trade_id="trade-1")

        self.assertEqual(context["selected_trade"]["run_id"], "ema-run")
        self.assertEqual(context["selected_trade"]["trade_id"], "trade-1")
        self.assertEqual(context["selected_trade"]["exit_reason"], "trend_break")
        self.assertEqual(context["chart_overlay"]["waveLine"], [])
        self.assertEqual([item["text"] for item in context["chart_overlay"]["markers"]], ["BUY", "SELL"])
        self.assertGreater(len(context["chart_rows"]), 0)


if __name__ == "__main__":
    unittest.main()
