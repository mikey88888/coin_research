from __future__ import annotations

import unittest
from unittest.mock import patch

import psycopg
from starlette.requests import Request

from coin_research.web.app import create_app
from coin_research.web.routes.pages import market_home


class PagesRouteTests(unittest.TestCase):
    def test_home_page_returns_200_when_market_database_is_unavailable(self) -> None:
        app = create_app()
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/",
                "headers": [],
                "query_string": b"",
                "app": app,
                "router": app.router,
                "scheme": "http",
                "server": ("testserver", 80),
                "client": ("testclient", 5000),
                "root_path": "",
                "path_params": {},
            }
        )
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


if __name__ == "__main__":
    unittest.main()
