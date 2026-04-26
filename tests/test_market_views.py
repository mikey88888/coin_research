from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd
import psycopg

from coin_research.services.market_views import build_asset_detail_context, build_market_home_context


class MarketViewsTests(unittest.TestCase):
    def test_market_home_exposes_active_leaderboard(self) -> None:
        summary = {
            "tracked_symbols": 2,
            "total_rows": 20,
            "latest_sync_at": pd.Timestamp("2026-01-01 00:00:00", tz="UTC"),
            "timeframes": [
                {
                    "timeframe": "1d",
                    "rows": 20,
                    "symbol_count": 2,
                    "first_bar": pd.Timestamp("2025-01-01 00:00:00", tz="UTC"),
                    "last_bar": pd.Timestamp("2026-01-01 00:00:00", tz="UTC"),
                }
            ],
        }
        cards = pd.DataFrame(
            [
                {
                    "symbol": "BTC/USDT",
                    "rows_1d": 10,
                    "rows_4h": 20,
                    "rows_30m": 30,
                    "rows_5m": 40,
                    "latest_sync_at": pd.Timestamp("2026-01-01 00:00:00", tz="UTC"),
                }
            ]
        )
        leaderboard = [
            {
                "rank": 1,
                "strategy_label": "五浪加速下跌反转",
                "timeframe": "30m",
                "exit_mode": "three_wave_exit",
                "return_drawdown_ratio": 3.2,
                "annualized_return_pct": 120.0,
                "max_drawdown_pct": 37.5,
                "run_id": "run-1",
                "detail_url": "/research/runs/run-1",
            }
        ]

        with patch("coin_research.services.market_views.load_market_summary", return_value=summary), patch(
            "coin_research.services.market_views.load_symbol_cards", return_value=cards
        ), patch("coin_research.services.market_views.list_backtest_runs", return_value=[]), patch(
            "coin_research.services.market_views.load_active_leaderboard", return_value=leaderboard
        ):
            context = build_market_home_context()

        self.assertEqual(context["page_title"], "量化研究总览")
        self.assertEqual(context["leaderboard_rows"], leaderboard)
        self.assertEqual(context["featured_symbols"][0]["symbol"], "BTC/USDT")

    def test_market_home_degrades_when_database_unavailable(self) -> None:
        leaderboard = [{"rank": 1, "run_id": "run-1"}]
        with patch(
            "coin_research.services.market_views.load_market_summary",
            side_effect=psycopg.OperationalError("connection failed"),
        ), patch("coin_research.services.market_views.list_backtest_runs", return_value=[]), patch(
            "coin_research.services.market_views.load_active_leaderboard", return_value=leaderboard
        ):
            context = build_market_home_context()

        self.assertIn("市场数据暂不可用", context["market_data_error"])
        self.assertEqual(context["market_summary"]["tracked_symbols"], 0)
        self.assertEqual(context["leaderboard_rows"], leaderboard)
        self.assertEqual(context["featured_symbols"], [])

    def test_asset_detail_rejects_unsupported_timeframe_with_readable_error(self) -> None:
        with patch("coin_research.services.market_views.load_ohlcv") as mocked_load:
            with self.assertRaisesRegex(ValueError, r"unsupported timeframe: '15m'; expected one of \[1d, 4h, 30m, 5m\]"):
                build_asset_detail_context("BTC/USDT", timeframe="15m")
        mocked_load.assert_not_called()

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
        self.assertEqual([item["text"] for item in context["chart_overlay"]["markers"]], ["买入", "卖出 · trend_break"])
        self.assertGreater(len(context["chart_rows"]), 0)


if __name__ == "__main__":
    unittest.main()
