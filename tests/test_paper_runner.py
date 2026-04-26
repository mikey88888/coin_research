from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from coin_research.live.runner import main, run_session


class PaperRunnerTests(unittest.TestCase):
    def test_run_session_marks_failed_when_universe_bootstrap_raises(self) -> None:
        fake_conn = MagicMock()
        session = {
            "session_id": "paper-1",
            "exchange": "binance",
            "quote": "USDT",
            "timeframe": "30m",
            "initial_capital": 100000.0,
            "cash": 100000.0,
            "peak_equity": 100000.0,
            "top_n": 20,
            "max_positions": 5,
            "position_target_pct": 0.2,
            "max_gross_exposure_pct": 1.0,
            "fee_rate": 0.001,
            "quantity_step": 0.0001,
            "lookback_bars": 60,
            "volatility_window": 60,
            "hold_bars": 5,
            "top_k": 5,
            "rebalance_interval": 5,
            "min_volatility_pct": 0.5,
            "min_momentum_pct": 5.0,
        }
        with patch("coin_research.live.runner.connect_pg", return_value=fake_conn), patch(
            "coin_research.live.runner.ensure_schema"
        ), patch("coin_research.live.runner.load_session", return_value=session), patch(
            "coin_research.live.runner.create_exchange", return_value=object()
        ), patch(
            "coin_research.live.runner._load_or_snapshot_universe", side_effect=RuntimeError("network timeout")
        ), patch("coin_research.live.runner.add_event") as mocked_event, patch(
            "coin_research.live.runner.mark_session_failed"
        ) as mocked_failed:
            with self.assertRaisesRegex(RuntimeError, "network timeout"):
                run_session(session_id="paper-1", poll_seconds=1)

        mocked_event.assert_called_once()
        mocked_failed.assert_called_once()

    def test_runner_main_exits_nonzero_without_rethrowing_traceback(self) -> None:
        with patch("coin_research.live.runner.run_session", side_effect=RuntimeError("boom")), patch(
            "sys.argv", ["paper-runner", "--session-id", "paper-1"]
        ):
            with self.assertRaises(SystemExit) as exc:
                main()
        self.assertEqual(exc.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
