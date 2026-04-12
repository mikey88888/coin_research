from __future__ import annotations

import unittest
from unittest.mock import patch

from coin_research.config import load_settings


class ConfigTests(unittest.TestCase):
    def test_load_settings_from_environment(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "COIN_RESEARCH_EXCHANGE": "okx",
                "COIN_RESEARCH_API_KEY": "demo-key",
                "COIN_RESEARCH_API_SECRET": "demo-secret",
                "COIN_RESEARCH_ENABLE_RATE_LIMIT": "false",
                "COIN_RESEARCH_TIMEOUT_MS": "15000",
            },
            clear=False,
        ):
            settings = load_settings()
        self.assertEqual(settings.exchange, "okx")
        self.assertEqual(settings.api_key, "demo-key")
        self.assertEqual(settings.api_secret, "demo-secret")
        self.assertFalse(settings.enable_rate_limit)
        self.assertEqual(settings.timeout_ms, 15000)


if __name__ == "__main__":
    unittest.main()
