from __future__ import annotations

import argparse
from contextlib import redirect_stderr, redirect_stdout
import io
import sys
import unittest
from unittest.mock import patch

import pandas as pd

from coin_research.cli import _build_config, main
from coin_research.config import ExchangeConfig


class CliValidationTests(unittest.TestCase):
    def _run_main(self, *argv: str) -> tuple[int, str]:
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["coin-research", *argv]), redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as exc:
                main()
        return exc.exception.code, stderr.getvalue()

    def _run_main_stdout(self, *argv: str) -> tuple[str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["coin-research", *argv]), redirect_stdout(stdout), redirect_stderr(stderr):
            main()
        return stdout.getvalue(), stderr.getvalue()

    def test_markets_limit_requires_positive_integer(self) -> None:
        code, stderr = self._run_main("markets", "--limit", "0")
        self.assertEqual(code, 2)
        self.assertIn("argument --limit: must be a positive integer, got 0", stderr)

    def test_ohlcv_limit_requires_positive_integer(self) -> None:
        code, stderr = self._run_main("ohlcv", "--symbol", "BTC/USDT", "--limit", "-5")
        self.assertEqual(code, 2)
        self.assertIn("argument --limit: must be a positive integer, got -5", stderr)

    def test_ohlcv_since_requires_non_negative_integer(self) -> None:
        code, stderr = self._run_main("ohlcv", "--symbol", "BTC/USDT", "--since", "-1")
        self.assertEqual(code, 2)
        self.assertIn("argument --since: must be a non-negative integer, got -1", stderr)
        self.assertNotIn("Traceback", stderr)

    def test_ohlcv_rejects_blank_symbol_cleanly(self) -> None:
        with patch(
            "coin_research.cli.load_settings",
            return_value=ExchangeConfig(exchange="binance", api_key=None, api_secret=None, enable_rate_limit=True, timeout_ms=10000),
        ):
            code, stderr = self._run_main("ohlcv", "--symbol", "   ")
        self.assertEqual(code, 2)
        self.assertIn("--symbol must not be blank", stderr)
        self.assertNotIn("Traceback", stderr)

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

    def test_build_config_normalizes_cli_exchange_override(self) -> None:
        with patch(
            "coin_research.cli.load_settings",
            return_value=ExchangeConfig(exchange="binance", api_key=None, api_secret=None, enable_rate_limit=True, timeout_ms=10000),
        ):
            config = _build_config(argparse.Namespace(exchange=" OKX "))
        self.assertEqual(config.exchange, "okx")

    def test_cli_rejects_blank_exchange_override(self) -> None:
        with patch(
            "coin_research.cli.load_settings",
            return_value=ExchangeConfig(exchange="binance", api_key=None, api_secret=None, enable_rate_limit=True, timeout_ms=10000),
        ):
            code, stderr = self._run_main("markets", "--exchange", "   ")
        self.assertEqual(code, 2)
        self.assertIn("--exchange must not be blank", stderr)
        self.assertNotIn("Traceback", stderr)

    def test_markets_normalizes_quote_filter(self) -> None:
        markets = pd.DataFrame(
            [
                {"symbol": "BTC/USDT", "base": "BTC", "quote": "USDT", "spot": True},
                {"symbol": "ETH/BTC", "base": "ETH", "quote": "BTC", "spot": True},
            ]
        )
        with patch(
            "coin_research.cli.load_settings",
            return_value=ExchangeConfig(exchange="binance", api_key=None, api_secret=None, enable_rate_limit=True, timeout_ms=10000),
        ), patch("coin_research.cli.list_markets", return_value=markets):
            stdout, stderr = self._run_main_stdout("markets", "--quote", " usdt ", "--limit", "5")
        self.assertIn("BTC/USDT", stdout)
        self.assertNotIn("ETH/BTC", stdout)
        self.assertEqual(stderr, "")

    def test_ohlcv_normalizes_symbol_before_fetch(self) -> None:
        frame = pd.DataFrame(
            [
                {"open_time": "2024-01-01T00:00:00Z", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0}
            ]
        )
        with patch(
            "coin_research.cli.load_settings",
            return_value=ExchangeConfig(exchange="binance", api_key=None, api_secret=None, enable_rate_limit=True, timeout_ms=10000),
        ), patch("coin_research.cli.fetch_ohlcv_frame", return_value=frame) as mocked_fetch:
            stdout, stderr = self._run_main_stdout("ohlcv", "--symbol", " BTC/USDT ")
        self.assertIn("rows=1", stdout)
        self.assertEqual(stderr, "")
        self.assertEqual(mocked_fetch.call_args.kwargs["symbol"], "BTC/USDT")

    def test_cli_rejects_blank_quote_cleanly(self) -> None:
        with patch(
            "coin_research.cli.load_settings",
            return_value=ExchangeConfig(exchange="binance", api_key=None, api_secret=None, enable_rate_limit=True, timeout_ms=10000),
        ):
            code, stderr = self._run_main("sync-top", "--quote", "   ")
        self.assertEqual(code, 2)
        self.assertIn("--quote must not be blank", stderr)
        self.assertNotIn("Traceback", stderr)


if __name__ == "__main__":
    unittest.main()
