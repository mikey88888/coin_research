from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from coin_research import db


class FakeCursor:
    def __init__(self, results: list[dict] | None = None):
        self.results = list(results or [])
        self.current: dict = {}
        self.execute_calls: list[tuple[str, object]] = []
        self.executemany_calls: list[tuple[str, list[tuple]]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.execute_calls.append((query, params))
        self.current = self.results.pop(0) if self.results else {}

    def executemany(self, query, rows):
        self.executemany_calls.append((query, list(rows)))
        self.current = {}

    def fetchone(self):
        return self.current.get("one")

    def fetchall(self):
        return self.current.get("all", [])

    @property
    def description(self):
        return [SimpleNamespace(name=name) for name in self.current.get("description", [])]


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


class DbModuleTests(unittest.TestCase):
    def test_get_pg_dsn_requires_env(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "COIN_RESEARCH_PG_DSN is not set"):
                db.get_pg_dsn()

        with patch.dict(os.environ, {"COIN_RESEARCH_PG_DSN": "postgresql://demo"}):
            self.assertEqual(db.get_pg_dsn(), "postgresql://demo")

    def test_connect_pg_sets_timezone_and_jit(self) -> None:
        cursor = FakeCursor()
        conn = FakeConnection(cursor)
        with patch("coin_research.db.psycopg.connect", return_value=conn) as mocked:
            result = db.connect_pg("postgresql://demo")
        self.assertIs(result, conn)
        mocked.assert_called_once_with("postgresql://demo")
        self.assertEqual(cursor.execute_calls[0][0], "SET TIME ZONE 'UTC'")
        self.assertEqual(cursor.execute_calls[1][0], "SET jit = off")

    def test_ensure_schema_executes_each_statement(self) -> None:
        cursor = FakeCursor()
        conn = FakeConnection(cursor)
        db.ensure_schema(conn)
        self.assertGreater(len(cursor.execute_calls), 3)
        self.assertEqual(conn.commits, 1)

    def test_nullable_and_fetch_dataframe(self) -> None:
        ts = pd.Timestamp("2026-04-06 10:30:00+00:00")
        self.assertIsNone(db._nullable(float("nan")))
        self.assertEqual(db._nullable(ts), ts.to_pydatetime())
        self.assertEqual(db._nullable(3), 3)

        cursor = FakeCursor([{"all": [(1, "x")], "description": ["id", "name"]}])
        conn = FakeConnection(cursor)
        with patch("coin_research.db.connect_pg", return_value=conn):
            frame = db._fetch_dataframe("select 1", params=["demo"])
        self.assertEqual(frame.to_dict(orient="records"), [{"id": 1, "name": "x"}])
        self.assertEqual(cursor.execute_calls[0], ("select 1", ["demo"]))

    def test_upsert_and_load_helpers(self) -> None:
        cursor = FakeCursor([{"one": [pd.Timestamp("2026-04-06 10:00:00+00:00")]}])
        conn = FakeConnection(cursor)

        markets = pd.DataFrame(
            [
                {
                    "symbol": "BTC/USDT",
                    "base": "BTC",
                    "quote": "USDT",
                    "type": "spot",
                    "spot": True,
                    "swap": False,
                    "future": False,
                    "active": True,
                }
            ]
        )
        count = db.upsert_markets(conn, markets, exchange_name="binance")
        self.assertEqual(count, 1)
        self.assertEqual(conn.commits, 1)
        self.assertIn("INSERT INTO market_data.crypto_markets", cursor.executemany_calls[0][0])

        bars = pd.DataFrame(
            [
                {
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "timeframe": "5m",
                    "datetime": pd.Timestamp("2026-04-06 10:00:00+00:00"),
                    "open": 100.0,
                    "high": 110.0,
                    "low": 90.0,
                    "close": 105.0,
                    "volume": 12.5,
                }
            ]
        )
        count = db.upsert_ohlcv(conn, bars)
        self.assertEqual(count, 1)
        self.assertEqual(conn.commits, 2)
        self.assertIn("INSERT INTO market_data.crypto_ohlcv", cursor.executemany_calls[1][0])

        latest = db.get_latest_bar_time(conn, exchange_name="binance", symbol="BTC/USDT", timeframe="5m")
        self.assertEqual(str(latest), "2026-04-06 10:00:00+00:00")

        sample = pd.DataFrame([{"symbol": "BTC/USDT"}])
        with patch("coin_research.db._fetch_dataframe", return_value=sample) as mocked:
            self.assertIs(db.load_markets(), sample)
            self.assertIs(db.load_ohlcv(exchange_name="binance", symbol="BTC/USDT", timeframe="5m"), sample)
        self.assertEqual(mocked.call_count, 2)


if __name__ == "__main__":
    unittest.main()
