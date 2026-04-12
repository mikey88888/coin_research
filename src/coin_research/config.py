from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dependency installed after `uv sync`
    def load_dotenv(*_args, **_kwargs) -> bool:
        return False


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _as_positive_int(value: str | None, *, default: int, env_name: str) -> int:
    if value is None:
        return default
    raw = value.strip()
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(f"{env_name} must be an integer, got {raw!r}") from exc
    if parsed <= 0:
        raise ValueError(f"{env_name} must be > 0, got {parsed}")
    return parsed


@dataclass(frozen=True)
class ExchangeConfig:
    exchange: str = "binance"
    api_key: str | None = None
    api_secret: str | None = None
    enable_rate_limit: bool = True
    timeout_ms: int = 10000


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_project_env() -> bool:
    return load_dotenv(project_root() / ".env")


def load_settings() -> ExchangeConfig:
    load_project_env()
    return ExchangeConfig(
        exchange=os.getenv("COIN_RESEARCH_EXCHANGE", "binance"),
        api_key=os.getenv("COIN_RESEARCH_API_KEY") or None,
        api_secret=os.getenv("COIN_RESEARCH_API_SECRET") or None,
        enable_rate_limit=_as_bool(os.getenv("COIN_RESEARCH_ENABLE_RATE_LIMIT"), True),
        timeout_ms=_as_positive_int(
            os.getenv("COIN_RESEARCH_TIMEOUT_MS"),
            default=10000,
            env_name="COIN_RESEARCH_TIMEOUT_MS",
        ),
    )
