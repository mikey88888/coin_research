from __future__ import annotations

from datetime import UTC, datetime
import unittest
from unittest.mock import patch

import pandas as pd

from coin_research.live.paper import (
    MarketRule,
    PaperPosition,
    PaperSignal,
    PaperTradingConfig,
    apply_execution,
    build_default_config,
    is_session_stale,
    select_signals_for_time,
)


def _frame_for_symbol(symbol: str, *, drift: float) -> pd.DataFrame:
    bars = 66
    times = pd.date_range("2026-01-01", periods=bars, freq="30min", tz="UTC")
    close = [100.0 + drift * idx for idx in range(bars)]
    return pd.DataFrame(
        {
            "symbol": [symbol] * bars,
            "bar_time": times,
            "open": [value - 0.2 for value in close],
            "high": [value + 0.5 for value in close],
            "low": [value - 0.5 for value in close],
            "close": close,
            "volume": [1_000 + idx for idx in range(bars)],
        }
    )


class PaperTradingTests(unittest.TestCase):
    def test_default_paper_config_matches_v1_validation_contract(self) -> None:
        config = PaperTradingConfig()
        validated = config.validate()
        self.assertEqual(validated.hold_bars, 5)
        self.assertEqual(validated.top_k, 5)
        self.assertEqual(validated.rebalance_interval, 5)
        self.assertEqual(validated.min_momentum_pct, 5.0)

    def test_select_signals_for_time_returns_ranked_top_k(self) -> None:
        config = build_default_config(timeframe="30m", top_n=20, initial_capital=100000)
        market_frames = {
            "AAA/USDT": _frame_for_symbol("AAA/USDT", drift=0.10),
            "BBB/USDT": _frame_for_symbol("BBB/USDT", drift=0.20),
            "CCC/USDT": _frame_for_symbol("CCC/USDT", drift=0.30),
            "DDD/USDT": _frame_for_symbol("DDD/USDT", drift=0.40),
            "EEE/USDT": _frame_for_symbol("EEE/USDT", drift=0.50),
            "FFF/USDT": _frame_for_symbol("FFF/USDT", drift=0.60),
        }

        signal_time = pd.Timestamp("2026-01-02 08:00:00+00:00")
        signals = select_signals_for_time(market_frames, signal_time=signal_time, config=config)

        self.assertEqual(len(signals), 5)
        self.assertEqual(signals[0].symbol, "FFF/USDT")
        self.assertEqual(signals[-1].rank, 5)
        self.assertTrue(all(item.entry_time > item.signal_time for item in signals))

    def test_apply_execution_buys_then_closes_due_position(self) -> None:
        config = build_default_config(timeframe="30m", top_n=20, initial_capital=1000)
        signal = PaperSignal(
            signal_id="sig-1",
            symbol="BTC/USDT",
            signal_time=pd.Timestamp("2026-01-01 00:00:00+00:00"),
            entry_time=pd.Timestamp("2026-01-01 00:30:00+00:00"),
            planned_exit_time=pd.Timestamp("2026-01-01 03:00:00+00:00"),
            entry_price=100.0,
            momentum_pct=8.0,
            volatility_pct=1.0,
            score=8.0,
            rank=1,
        )
        result = apply_execution(
            execution_time=signal.entry_time,
            session_id="paper-1",
            positions={},
            signals=[signal],
            open_prices={"BTC/USDT": 100.0},
            cash=1000.0,
            peak_equity=1000.0,
            config=config,
            market_rules={"BTC/USDT": MarketRule(quantity_step=0.0001, min_notional=10.0)},
        )

        self.assertEqual(len(result.orders), 1)
        self.assertIn("BTC/USDT", result.positions)
        self.assertLess(result.cash, 1000.0)

        exit_result = apply_execution(
            execution_time=signal.planned_exit_time,
            session_id="paper-1",
            positions=result.positions,
            signals=[],
            open_prices={"BTC/USDT": 105.0},
            cash=result.cash,
            peak_equity=result.peak_equity,
            config=config,
            market_rules={"BTC/USDT": MarketRule(quantity_step=0.0001, min_notional=10.0)},
        )
        self.assertEqual(len(exit_result.orders), 1)
        self.assertEqual(exit_result.orders[0].side, "sell")
        self.assertEqual(exit_result.positions, {})
        self.assertGreater(exit_result.cash, result.cash)

    def test_is_session_stale_requires_dead_process_and_old_heartbeat(self) -> None:
        session = {
            "status": "running",
            "pid": 12345,
            "heartbeat_at": pd.Timestamp("2026-01-01 00:00:00+00:00"),
        }
        with patch("coin_research.live.paper.is_process_alive", return_value=False):
            stale = is_session_stale(session, now=datetime(2026, 1, 1, 0, 20, tzinfo=UTC))
        self.assertTrue(stale)


if __name__ == "__main__":
    unittest.main()
