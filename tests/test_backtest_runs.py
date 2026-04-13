from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from coin_research.services.backtest_runs import build_leaderboard_context, load_active_leaderboard


class BacktestRunsTests(unittest.TestCase):
    def test_load_active_leaderboard_reads_top_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            research_dir = root / "research"
            research_dir.mkdir(parents=True, exist_ok=True)
            (research_dir / "leaderboard.json").write_text(
                json.dumps(
                    {
                        "active_top_results": [
                            {
                                "rank": 1,
                                "strategy_key": "five-wave-reversal",
                                "strategy_label": "五浪加速下跌反转",
                                "timeframe": "30m",
                                "exit_mode": "three_wave_exit",
                                "engine_type": "account",
                                "run_id": "run-1",
                                "annualized_return_pct": 120.5,
                                "total_return_pct": 380.1,
                                "max_drawdown_pct": 33.2,
                                "closed_trades": 88,
                                "win_rate": 55.5,
                                "return_drawdown_ratio": 3.62,
                                "stability": "stable",
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            rows = load_active_leaderboard(root=root)
            context = build_leaderboard_context(root=root)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["strategy_label"], "五浪加速下跌反转")
        self.assertEqual(rows[0]["detail_url"], "/research/runs/run-1")
        self.assertTrue(context["has_rows"])
        self.assertEqual(context["leaderboard_rows"][0]["rank"], 1)


if __name__ == "__main__":
    unittest.main()
