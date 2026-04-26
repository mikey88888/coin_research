from __future__ import annotations

from contextlib import redirect_stderr
import io
import sys
import unittest
from unittest.mock import patch

from coin_research.web.app import main


class WebAppCliTests(unittest.TestCase):
    def test_web_app_rejects_blank_host_cleanly(self) -> None:
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["coin-research-web", "--host", "   "]), redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as exc:
                main()
        self.assertEqual(exc.exception.code, 2)
        self.assertIn("argument --host: must not be blank", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_web_app_rejects_non_integer_port_cleanly(self) -> None:
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["coin-research-web", "--port", "abc"]), redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as exc:
                main()
        self.assertEqual(exc.exception.code, 2)
        self.assertIn("argument --port: must be an integer, got 'abc'", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_web_app_requires_positive_port(self) -> None:
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["coin-research-web", "--port", "0"]), redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as exc:
                main()
        self.assertEqual(exc.exception.code, 2)
        self.assertIn("argument --port: must be a positive integer, got 0", stderr.getvalue())

    def test_web_app_rejects_port_above_u16_range(self) -> None:
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["coin-research-web", "--port", "70000"]), redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as exc:
                main()
        self.assertEqual(exc.exception.code, 2)
        self.assertIn("argument --port: must be <= 65535, got 70000", stderr.getvalue())

    def test_web_app_passes_valid_args_to_uvicorn(self) -> None:
        with patch.object(sys, "argv", ["coin-research-web", "--host", " 127.0.0.1 ", "--port", "9000", "--reload"]), patch(
            "coin_research.web.app.uvicorn.run"
        ) as mocked_run:
            main()
        mocked_run.assert_called_once_with(
            "coin_research.web.app:create_app",
            factory=True,
            host="127.0.0.1",
            port=9000,
            reload=True,
        )


if __name__ == "__main__":
    unittest.main()
