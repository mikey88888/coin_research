from __future__ import annotations

from contextlib import redirect_stderr
import io
import sys
import unittest
from unittest.mock import patch

from coin_research.cli import main


class CliValidationTests(unittest.TestCase):
    def _run_main(self, *argv: str) -> tuple[int, str]:
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["coin-research", *argv]), redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as exc:
                main()
        return exc.exception.code, stderr.getvalue()

    def test_markets_limit_requires_positive_integer(self) -> None:
        code, stderr = self._run_main("markets", "--limit", "0")
        self.assertEqual(code, 2)
        self.assertIn("argument --limit: must be a positive integer, got 0", stderr)

    def test_ohlcv_limit_requires_positive_integer(self) -> None:
        code, stderr = self._run_main("ohlcv", "--symbol", "BTC/USDT", "--limit", "-5")
        self.assertEqual(code, 2)
        self.assertIn("argument --limit: must be a positive integer, got -5", stderr)

    def test_sync_top_requires_positive_top_value(self) -> None:
        code, stderr = self._run_main("sync-top", "--top", "0")
        self.assertEqual(code, 2)
        self.assertIn("argument --top: must be a positive integer, got 0", stderr)

    def test_sync_top_requires_positive_symbols_limit(self) -> None:
        code, stderr = self._run_main("sync-top", "--symbols-limit", "-2")
        self.assertEqual(code, 2)
        self.assertIn("argument --symbols-limit: must be a positive integer, got -2", stderr)

    def test_cli_surfaces_config_validation_errors_cleanly(self) -> None:
        with patch("coin_research.cli.load_settings", side_effect=ValueError("COIN_RESEARCH_TIMEOUT_MS must be > 0, got 0")):
            code, stderr = self._run_main("markets")
        self.assertEqual(code, 2)
        self.assertIn("COIN_RESEARCH_TIMEOUT_MS must be > 0, got 0", stderr)
        self.assertNotIn("Traceback", stderr)


if __name__ == "__main__":
    unittest.main()
