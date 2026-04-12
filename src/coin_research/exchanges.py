from __future__ import annotations

from typing import Any

from .config import ExchangeConfig


def create_exchange(config: ExchangeConfig):
    try:
        import ccxt  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on local environment
        raise RuntimeError("ccxt is not installed. Run `uv sync` in coin_research first.") from exc

    if not hasattr(ccxt, config.exchange):
        raise ValueError(f"unsupported exchange: {config.exchange}")

    exchange_cls = getattr(ccxt, config.exchange)
    options: dict[str, Any] = {
        "enableRateLimit": config.enable_rate_limit,
        "timeout": config.timeout_ms,
    }
    if config.api_key:
        options["apiKey"] = config.api_key
    if config.api_secret:
        options["secret"] = config.api_secret
    exchange = exchange_cls(options)
    # Let ccxt inherit HTTP(S)_PROXY from the shell environment inside WSL.
    exchange.session.trust_env = True
    return exchange
