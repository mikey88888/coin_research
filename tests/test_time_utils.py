from __future__ import annotations

import unittest

import pandas as pd

from coin_research.time_utils import format_beijing_ts, to_beijing_timestamp
from coin_research.web.templating import TEMPLATES


class TimeUtilsTests(unittest.TestCase):
    def test_format_beijing_ts_converts_utc_input(self) -> None:
        self.assertEqual(
            format_beijing_ts(pd.Timestamp("2026-04-26 08:30:00+00:00"), seconds=True),
            "2026-04-26 16:30:00 北京时间",
        )

    def test_to_beijing_timestamp_returns_none_for_invalid_input(self) -> None:
        self.assertIsNone(to_beijing_timestamp(""))
        self.assertIsNone(to_beijing_timestamp("not-a-time"))

    def test_template_format_ts_uses_beijing_time(self) -> None:
        output = TEMPLATES.env.filters["format_ts"]("2026-04-26T09:00:00+00:00")
        self.assertEqual(output, "2026-04-26 17:00 北京时间")


if __name__ == "__main__":
    unittest.main()
