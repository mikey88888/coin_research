from __future__ import annotations

import unittest

from coin_research.backtests.account import AccountConfig, run_account_backtest


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


if __name__ == "__main__":
    unittest.main()
