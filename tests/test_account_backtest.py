from __future__ import annotations

import unittest

import pandas as pd

from coin_research.backtests.account import AccountConfig, run_account_backtest
from coin_research.backtests.short_account import run_short_account_backtest
from coin_research.strategies.absolute_momentum_volatility_composite import AbsoluteMomentumVolatilityCompositeTrade


class AccountBacktestTests(unittest.TestCase):
    def test_run_account_backtest_rejects_zero_quantity_step(self) -> None:
        with self.assertRaisesRegex(ValueError, "quantity_step must be > 0"):
            run_account_backtest(
                run_id="demo",
                signals=[],
                market_frames={},
                time_column="bar_time",
                config=AccountConfig(quantity_step=0),
            )

    def test_run_account_backtest_rejects_negative_quantity_step(self) -> None:
        with self.assertRaisesRegex(ValueError, "quantity_step must be > 0"):
            run_account_backtest(
                run_id="demo",
                signals=[],
                market_frames={},
                time_column="bar_time",
                config=AccountConfig(quantity_step=-0.01),
            )

    def test_run_short_account_backtest_marks_short_profit(self) -> None:
        signal = self._signal(entry_price=100.0, exit_price=80.0)
        market = pd.DataFrame(
            [
                {"bar_time": "2024-01-01T00:00:00Z", "open": 100.0, "close": 100.0},
                {"bar_time": "2024-01-02T00:00:00Z", "open": 80.0, "close": 80.0},
            ]
        )

        result = run_short_account_backtest(
            run_id="short-demo",
            signals=[signal],
            market_frames={"BTC/USDT": market},
            time_column="bar_time",
            config=AccountConfig(
                initial_capital=1000.0,
                position_target_pct=0.5,
                max_positions=1,
                max_gross_exposure_pct=1.0,
                fee_rate=0.0,
                quantity_step=0.01,
            ),
        )

        self.assertEqual(result.orders[0].side, "sell_short")
        self.assertEqual(result.orders[1].side, "buy_to_cover")
        self.assertAlmostEqual(result.trades[0].pnl_amount, 100.0)
        self.assertAlmostEqual(result.trades[0].return_pct, 20.0)
        self.assertAlmostEqual(result.summary["ending_equity"], 1100.0)
        self.assertAlmostEqual(result.summary["total_return_pct"], 10.0)

    def test_run_short_account_backtest_marks_short_loss(self) -> None:
        signal = self._signal(entry_price=100.0, exit_price=120.0)
        market = pd.DataFrame(
            [
                {"bar_time": "2024-01-01T00:00:00Z", "open": 100.0, "close": 100.0},
                {"bar_time": "2024-01-02T00:00:00Z", "open": 120.0, "close": 120.0},
            ]
        )

        result = run_short_account_backtest(
            run_id="short-loss-demo",
            signals=[signal],
            market_frames={"BTC/USDT": market},
            time_column="bar_time",
            config=AccountConfig(
                initial_capital=1000.0,
                position_target_pct=0.5,
                max_positions=1,
                fee_rate=0.0,
                quantity_step=0.01,
            ),
        )

        self.assertAlmostEqual(result.trades[0].pnl_amount, -100.0)
        self.assertAlmostEqual(result.trades[0].return_pct, -20.0)
        self.assertAlmostEqual(result.summary["ending_equity"], 900.0)

    def test_run_short_account_backtest_charges_entry_and_exit_fees(self) -> None:
        signal = self._signal(entry_price=100.0, exit_price=80.0)
        market = pd.DataFrame(
            [
                {"bar_time": "2024-01-01T00:00:00Z", "open": 100.0, "close": 100.0},
                {"bar_time": "2024-01-02T00:00:00Z", "open": 80.0, "close": 80.0},
            ]
        )

        result = run_short_account_backtest(
            run_id="short-fee-demo",
            signals=[signal],
            market_frames={"BTC/USDT": market},
            time_column="bar_time",
            config=AccountConfig(
                initial_capital=1000.0,
                position_target_pct=0.5,
                max_positions=1,
                fee_rate=0.01,
                quantity_step=0.01,
            ),
        )

        self.assertAlmostEqual(result.summary["total_fees_paid"], 9.0)
        self.assertAlmostEqual(result.summary["ending_equity"], 1091.0)

    @staticmethod
    def _signal(*, entry_price: float, exit_price: float) -> AbsoluteMomentumVolatilityCompositeTrade:
        return AbsoluteMomentumVolatilityCompositeTrade(
            signal_id="BTC/USDT-demo",
            symbol="BTC/USDT",
            wave_start_date=pd.Timestamp("2023-12-01T00:00:00Z"),
            wave_end_date=pd.Timestamp("2023-12-31T00:00:00Z"),
            p0_price=90.0,
            p1_price=100.0,
            p2_price=entry_price,
            p3_price=exit_price,
            p4_price=10.0,
            p5_price=2.0,
            p0_date=pd.Timestamp("2023-12-01T00:00:00Z"),
            p1_date=pd.Timestamp("2023-12-31T00:00:00Z"),
            p2_date=pd.Timestamp("2024-01-01T00:00:00Z"),
            p3_date=pd.Timestamp("2024-01-02T00:00:00Z"),
            p4_date=None,
            p5_date=None,
            wave_drop_pct=10.0,
            speed1=1.0,
            speed3=1.0,
            speed5=5.0,
            fractal_center_date=pd.Timestamp("2023-12-31T00:00:00Z"),
            signal_confirm_date=pd.Timestamp("2023-12-31T00:00:00Z"),
            entry_date=pd.Timestamp("2024-01-01T00:00:00Z"),
            entry_price=entry_price,
            planned_hold_bars=1,
            exit_date=pd.Timestamp("2024-01-02T00:00:00Z"),
            exit_price=exit_price,
            return_pct=((exit_price / entry_price) - 1.0) * 100.0,
            holding_days=1,
            status="closed",
            exit_reason="rebalance_exit",
            p0_index=0,
            p5_index=1,
            entry_index=2,
            exit_index=3,
        )


if __name__ == "__main__":
    unittest.main()
