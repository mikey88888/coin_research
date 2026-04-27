from __future__ import annotations

from contextlib import redirect_stderr
import io
import logging
import sys
import unittest
from unittest.mock import MagicMock, patch

from coin_research.live.runner import BeijingLogFormatter, main, run_session


class PaperRunnerTests(unittest.TestCase):
    def _run_main(self, *argv: str) -> tuple[int, str]:
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["paper-runner", *argv]), redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as exc:
                main()
        return exc.exception.code, stderr.getvalue()

    def test_run_session_rejects_non_positive_poll_seconds_early(self) -> None:
        with patch("coin_research.live.runner._configure_logging") as mocked_logging:
            with self.assertRaisesRegex(ValueError, "poll_seconds must be a positive integer, got 0"):
                run_session(session_id="paper-1", poll_seconds=0)
        mocked_logging.assert_not_called()

    def test_beijing_log_formatter_uses_readable_beijing_time(self) -> None:
        record = logging.LogRecord("paper", logging.INFO, __file__, 1, "message", (), None)
        record.created = 1_777_193_107.0

        output = BeijingLogFormatter("%(asctime)s %(message)s").format(record)

        self.assertIn("北京时间 message", output)

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

    def test_runner_main_rejects_blank_session_id_cleanly(self) -> None:
        code, stderr = self._run_main("--session-id", "   ")
        self.assertEqual(code, 2)
        self.assertIn("argument --session-id: must not be blank", stderr)
        self.assertNotIn("Traceback", stderr)

    def test_runner_main_rejects_non_positive_poll_seconds_cleanly(self) -> None:
        code, stderr = self._run_main("--session-id", "paper-1", "--poll-seconds", "0")
        self.assertEqual(code, 2)
        self.assertIn("argument --poll-seconds: must be a positive integer, got 0", stderr)
        self.assertNotIn("Traceback", stderr)

    def test_runner_main_exits_nonzero_without_rethrowing_traceback(self) -> None:
        with patch("coin_research.live.runner.run_session", side_effect=RuntimeError("boom")), patch(
            "sys.argv", ["paper-runner", "--session-id", "paper-1"]
        ):
            with self.assertRaises(SystemExit) as exc:
                main()
        self.assertEqual(exc.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
