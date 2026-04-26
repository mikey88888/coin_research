from __future__ import annotations

from pathlib import Path

from fastapi.templating import Jinja2Templates

from ..time_utils import format_beijing_ts

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = Jinja2Templates(directory=str(PACKAGE_ROOT / "templates"))
STATIC_ROOT = PACKAGE_ROOT / "static"


def _format_number(value, decimals: int = 2):
    if value is None or value == "":
        return "n/a"
    try:
        number = float(value)
    except Exception:
        return str(value)
    if decimals == 0:
        return f"{number:,.0f}"
    return f"{number:,.{decimals}f}"


def _format_int(value):
    if value is None or value == "":
        return "n/a"
    try:
        return f"{int(value):,}"
    except Exception:
        return str(value)


def _format_pct(value, decimals: int = 2):
    if value is None or value == "":
        return "n/a"
    try:
        return f"{float(value):,.{decimals}f}%"
    except Exception:
        return str(value)


def _format_ts(value):
    if value is None or value == "":
        return "n/a"
    return format_beijing_ts(value) or str(value)


TEMPLATES.env.filters["format_number"] = _format_number
TEMPLATES.env.filters["format_int"] = _format_int
TEMPLATES.env.filters["format_pct"] = _format_pct
TEMPLATES.env.filters["format_ts"] = _format_ts
