from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

import psycopg
from starlette.requests import Request

from coin_research.live.connectivity import BinanceConnectivityError
from coin_research.web.app import create_app
from coin_research.web.routes.pages import market_home, paper_dashboard, paper_start, paper_stop


class PagesRouteTests(unittest.TestCase):
    def _request(self, *, method: str, path: str, body: bytes = b"") -> Request:
        app = create_app()
        sent = {"done": False}

        async def receive():
            if sent["done"]:
                return {"type": "http.request", "body": b"", "more_body": False}
            sent["done"] = True
            return {"type": "http.request", "body": body, "more_body": False}

        return Request(
            {
                "type": "http",
                "method": method,
                "path": path,
                "headers": [(b"content-type", b"application/x-www-form-urlencoded")],
                "query_string": b"",
                "app": app,
                "router": app.router,
                "scheme": "http",
                "server": ("testserver", 80),
                "client": ("testclient", 5000),
                "root_path": "",
                "path_params": {},
                "http_version": "1.1",
            },
            receive,
        )

    def test_home_page_returns_200_when_market_database_is_unavailable(self) -> None:
        request = self._request(method="GET", path="/")
        with patch(
            "coin_research.services.market_views.load_market_summary",
            side_effect=psycopg.OperationalError("connection failed"),
        ), patch("coin_research.services.market_views.list_backtest_runs", return_value=[]), patch(
            "coin_research.services.market_views.load_active_leaderboard", return_value=[]
        ):
            response = market_home(request)

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("首页已降级显示", body)
        self.assertIn("市场数据暂不可用", body)

    def test_paper_dashboard_renders_service_context(self) -> None:
        request = self._request(method="GET", path="/paper")
        with patch(
            "coin_research.web.routes.pages.build_paper_dashboard_context",
            return_value={
                "page_title": "模拟盘控制台",
                "session": None,
                "positions": [],
                "orders": [],
                "equity_rows": [],
                "events": [],
                "timeframe_choices": ["30m"],
                "default_timeframe": "30m",
                "default_top_n": 20,
                "default_initial_capital": 100000,
                "form_timeframe": "30m",
                "form_top_n": "20",
                "form_initial_capital": "100000",
                "paper_error": None,
                "action_error": None,
            },
        ):
            response = paper_dashboard(request)
        self.assertEqual(response.status_code, 200)
        self.assertIn("真实行情模拟盘", response.body.decode("utf-8"))

    def test_paper_start_redirects_after_successful_launch(self) -> None:
        request = self._request(
            method="POST",
            path="/paper/start",
            body=b"timeframe=30m&top_n=20&initial_capital=100000",
        )
        with patch("coin_research.web.routes.pages.start_paper_session", return_value="paper-1"):
            response = asyncio.run(paper_start(request))
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/paper")

    def test_paper_start_renders_connectivity_report_on_preflight_failure(self) -> None:
        request = self._request(
            method="POST",
            path="/paper/start",
            body=b"timeframe=4h&top_n=7&initial_capital=123456",
        )
        report = {
            "ok": False,
            "summary": "Binance connectivity preflight failed: current HTTP(S)_PROXY is not usable from WSL.",
            "recommendation": "fix proxy",
            "proxy_env": [{"key": "HTTPS_PROXY", "value": "http://127.0.0.1:7897"}],
            "wsl_gateway": "172.20.160.1",
            "probes": [
                {
                    "name": "env_binance_ping",
                    "target": "https://api.binance.com/api/v3/ping",
                    "ok": False,
                    "elapsed_ms": 100,
                    "error": "timed out",
                }
            ],
        }
        with patch("coin_research.web.routes.pages.start_paper_session", side_effect=BinanceConnectivityError(report)):
            response = asyncio.run(paper_start(request))

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("Binance 连接诊断", body)
        self.assertIn("env_binance_ping", body)
        self.assertIn("HTTPS_PROXY=http://127.0.0.1:7897", body)
        self.assertIn('option value="4h" selected', body)
        self.assertIn('name="top_n" min="1" value="7"', body)
        self.assertIn('name="initial_capital" min="1" step="1" value="123456"', body)

    def test_paper_start_rejects_invalid_timeframe_with_readable_error(self) -> None:
        request = self._request(
            method="POST",
            path="/paper/start",
            body=b"timeframe=5m&top_n=20&initial_capital=100000",
        )
        with patch("coin_research.web.routes.pages.start_paper_session") as mocked_start:
            response = asyncio.run(paper_start(request))

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("timeframe must be one of [30m, 4h, 1d]", body)
        self.assertIn("5m", body)
        self.assertIn('<option value="5m" selected>无效值: 5m</option>', body)
        mocked_start.assert_not_called()

    def test_paper_start_preserves_other_submitted_values_on_validation_error(self) -> None:
        request = self._request(
            method="POST",
            path="/paper/start",
            body=b"timeframe=4h&top_n=abc&initial_capital=250000",
        )
        with patch("coin_research.web.routes.pages.start_paper_session") as mocked_start:
            response = asyncio.run(paper_start(request))

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('option value="4h" selected', body)
        self.assertIn('name="top_n" min="1" value="abc"', body)
        self.assertIn('name="initial_capital" min="1" step="1" value="250000"', body)
        mocked_start.assert_not_called()

    def test_paper_start_rejects_non_positive_top_n_with_readable_error(self) -> None:
        request = self._request(
            method="POST",
            path="/paper/start",
            body=b"timeframe=30m&top_n=0&initial_capital=100000",
        )
        with patch("coin_research.web.routes.pages.start_paper_session") as mocked_start:
            response = asyncio.run(paper_start(request))

        self.assertEqual(response.status_code, 200)
        self.assertIn("top_n must be a positive integer, got 0", response.body.decode("utf-8"))
        mocked_start.assert_not_called()

    def test_paper_start_rejects_non_positive_initial_capital_with_readable_error(self) -> None:
        request = self._request(
            method="POST",
            path="/paper/start",
            body=b"timeframe=30m&top_n=20&initial_capital=0",
        )
        with patch("coin_research.web.routes.pages.start_paper_session") as mocked_start:
            response = asyncio.run(paper_start(request))

        self.assertEqual(response.status_code, 200)
        self.assertIn("initial_capital must be a positive number, got 0.0", response.body.decode("utf-8"))
        mocked_start.assert_not_called()

    def test_paper_start_rejects_non_finite_initial_capital_with_readable_error(self) -> None:
        for raw_value in ("nan", "inf"):
            with self.subTest(raw_value=raw_value):
                request = self._request(
                    method="POST",
                    path="/paper/start",
                    body=f"timeframe=30m&top_n=20&initial_capital={raw_value}".encode("utf-8"),
                )
                with patch("coin_research.web.routes.pages.start_paper_session") as mocked_start:
                    response = asyncio.run(paper_start(request))

                self.assertEqual(response.status_code, 200)
                body = response.body.decode("utf-8")
                self.assertIn("initial_capital must be a finite positive number", body)
                self.assertIn(raw_value, body)
                mocked_start.assert_not_called()

    def test_paper_stop_redirects_after_successful_request(self) -> None:
        request = self._request(method="POST", path="/paper/stop", body=b"")
        with patch("coin_research.web.routes.pages.stop_paper_session", return_value="paper-1"):
            response = asyncio.run(paper_stop(request))
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/paper")


if __name__ == "__main__":
    unittest.main()
