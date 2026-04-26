from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from coin_research.services.paper import start_paper_session


class PaperServiceTests(unittest.TestCase):
    def test_start_paper_session_fails_fast_when_binance_preflight_fails(self) -> None:
        fake_conn = MagicMock()
        fake_conn.__enter__.return_value = fake_conn
        fake_conn.__exit__.return_value = False
        with patch(
            "coin_research.services.paper._binance_connectivity_preflight",
            side_effect=RuntimeError("Binance connectivity preflight failed"),
        ), patch("coin_research.services.paper.connect_pg", return_value=fake_conn), patch(
            "coin_research.services.paper.create_session"
        ) as mocked_create, patch("coin_research.services.paper.subprocess.Popen") as mocked_popen:
            with self.assertRaisesRegex(RuntimeError, "Binance connectivity preflight failed"):
                start_paper_session(timeframe="30m", top_n=20, initial_capital=100000)

        mocked_create.assert_not_called()
        mocked_popen.assert_not_called()

    def test_start_paper_session_writes_launch_log_and_spawns_runner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            log_path = root / "log" / "paper_trading" / "paper-1.log"
            fake_conn = MagicMock()
            fake_conn.__enter__.return_value = fake_conn
            fake_conn.__exit__.return_value = False

            with patch("coin_research.services.paper.connect_pg", return_value=fake_conn), patch(
                "coin_research.services.paper.ensure_schema"
            ), patch("coin_research.services.paper.load_active_session", return_value=None), patch(
                "coin_research.services.paper.create_session", return_value="paper-1"
            ), patch(
                "coin_research.services.paper._binance_connectivity_preflight"
            ), patch("coin_research.services.paper.add_event"), patch(
                "coin_research.services.paper.project_root", return_value=root
            ), patch(
                "coin_research.services.paper.paper_log_path", return_value=log_path
            ), patch("coin_research.services.paper.subprocess.Popen") as mocked_popen:
                session_id = start_paper_session(timeframe="30m", top_n=20, initial_capital=100000)

            self.assertEqual(session_id, "paper-1")
            self.assertTrue(log_path.exists())
            self.assertIn("launching paper runner", log_path.read_text(encoding="utf-8"))
            mocked_popen.assert_called_once()


if __name__ == "__main__":
    unittest.main()
