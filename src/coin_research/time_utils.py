from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


BEIJING_TZ = ZoneInfo("Asia/Shanghai")
BEIJING_LABEL = "北京时间"


def to_beijing_timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp).tz_convert(BEIJING_TZ)


def format_beijing_ts(value: Any, *, seconds: bool = False) -> str | None:
    timestamp = to_beijing_timestamp(value)
    if timestamp is None:
        return str(value) if value not in (None, "") else None
    pattern = "%Y-%m-%d %H:%M:%S" if seconds else "%Y-%m-%d %H:%M"
    return f"{timestamp.strftime(pattern)} {BEIJING_LABEL}"


def beijing_now() -> datetime:
    return datetime.now(tz=BEIJING_TZ)


def beijing_now_label() -> str:
    return f"{beijing_now().strftime('%Y-%m-%d %H:%M:%S')} {BEIJING_LABEL}"
