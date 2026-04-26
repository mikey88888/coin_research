from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from coin_research.rank_paired_inverse_short import _is_forward_account_run, _paired_row, _write_leaderboard


class PairedInverseRankingTests(unittest.TestCase):
    def test_paired_row_uses_raw_average_of_forward_and_inverse_scores(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            forward_summary = root / "forward.json"
            inverse_summary = root / "inverse.json"
            forward_summary.write_text(
                json.dumps({"return_drawdown_ratio": 1.2, "closed_trades": 30, "annualized_return_pct": 40, "max_drawdown_pct": 20}),
                encoding="utf-8",
            )
            inverse_summary.write_text(
                json.dumps({"return_drawdown_ratio": -0.4, "closed_trades": 25, "annualized_return_pct": -10, "max_drawdown_pct": 50}),
                encoding="utf-8",
            )

            row = _paired_row(
                rank=1,
                source_meta={
                    "run_id": "forward-run",
                    "strategy_key": "demo",
                    "strategy_label": "Demo",
                    "timeframe": "1d",
                    "summary_path": str(forward_summary),
                },
                inverse_meta={"run_id": "inverse-run", "summary_path": str(inverse_summary)},
            )

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["paired_return_drawdown_ratio"], 0.4)
        self.assertEqual(row["return_drawdown_ratio"], 0.4)
        self.assertEqual(row["stability"], "stable")

    def test_forward_filter_skips_signal_and_inverse_runs(self) -> None:
        self.assertTrue(_is_forward_account_run({"engine_type": "account", "strategy_key": "demo"}))
        self.assertFalse(_is_forward_account_run({"engine_type": "signal", "strategy_key": "demo"}))
        self.assertFalse(_is_forward_account_run({"engine_type": "account", "strategy_key": "demo-inverse-short"}))
        self.assertFalse(_is_forward_account_run({"engine_type": "account", "strategy_key": "demo", "source_run_id": "run-1"}))

    def test_write_leaderboard_keeps_top_10(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                {
                    "rank": index,
                    "strategy_key": f"s{index}",
                    "paired_return_drawdown_ratio": float(20 - index),
                    "return_drawdown_ratio": float(20 - index),
                }
                for index in range(12)
            ]
            _write_leaderboard(root, rows)
            payload = json.loads((root / "research" / "leaderboard.json").read_text(encoding="utf-8"))

        self.assertEqual(len(payload["active_top_results"]), 10)
        self.assertEqual(payload["policy"]["primary_metric_alias"], "paired_return_drawdown_ratio")


if __name__ == "__main__":
    unittest.main()
