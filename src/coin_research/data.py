from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from .config import ExchangeConfig
from .exchanges import create_exchange


OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]


def timeframe_to_milliseconds(timeframe: str) -> int:
    match = re.fullmatch(r"(\d+)([smhdwM])", timeframe)
    if not match:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    value = int(match.group(1))
    if value <= 0:
        raise ValueError(f"timeframe value must be > 0, got {timeframe!r}")
    unit = match.group(2)
    multipliers = {
        "s": 1_000,
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 604_800_000,
        "M": 2_592_000_000,
    }
    return value * multipliers[unit]


def _ohlcv_rows_to_frame(
    *,
    exchange_name: str,
    symbol: str,
    timeframe: str,
    rows: list[list],
) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=OHLCV_COLUMNS)
    if frame.empty:
        return pd.DataFrame(columns=["exchange", "symbol", "timeframe", *OHLCV_COLUMNS, "datetime"])
    frame["datetime"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
    frame.insert(0, "timeframe", timeframe)
    frame.insert(0, "symbol", symbol)
    frame.insert(0, "exchange", exchange_name)
    return frame


def fetch_ohlcv_frame_from_exchange(
    *,
    exchange,
    exchange_name: str,
    symbol: str,
    timeframe: str,
    limit: int = 500,
    since: int | None = None,
) -> pd.DataFrame:
    rows = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
    return _ohlcv_rows_to_frame(
        exchange_name=exchange_name,
        symbol=symbol,
        timeframe=timeframe,
        rows=rows,
    )


def fetch_ohlcv_frame(
    *,
    exchange_name: str,
    symbol: str,
    timeframe: str,
    limit: int = 500,
    since: int | None = None,
    config: ExchangeConfig | None = None,
) -> pd.DataFrame:
    exchange_config = config or ExchangeConfig(exchange=exchange_name)
    if exchange_config.exchange != exchange_name:
        exchange_config = ExchangeConfig(
            exchange=exchange_name,
            api_key=exchange_config.api_key,
            api_secret=exchange_config.api_secret,
            enable_rate_limit=exchange_config.enable_rate_limit,
            timeout_ms=exchange_config.timeout_ms,
        )
    exchange = create_exchange(exchange_config)
    return fetch_ohlcv_frame_from_exchange(
        exchange=exchange,
        exchange_name=exchange_name,
        symbol=symbol,
        timeframe=timeframe,
        limit=limit,
        since=since,
    )


def list_markets_from_exchange(*, exchange, exchange_name: str) -> pd.DataFrame:
    markets = exchange.load_markets()
    rows = []
    for symbol, payload in markets.items():
        rows.append(
            {
                "symbol": symbol,
                "base": payload.get("base"),
                "quote": payload.get("quote"),
                "type": payload.get("type"),
                "spot": payload.get("spot"),
                "swap": payload.get("swap"),
                "future": payload.get("future"),
                "active": payload.get("active"),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.sort_values(["quote", "base", "symbol"], na_position="last").reset_index(drop=True)


def list_markets(*, exchange_name: str, config: ExchangeConfig | None = None) -> pd.DataFrame:
    exchange_config = config or ExchangeConfig(exchange=exchange_name)
    if exchange_config.exchange != exchange_name:
        exchange_config = ExchangeConfig(
            exchange=exchange_name,
            api_key=exchange_config.api_key,
            api_secret=exchange_config.api_secret,
            enable_rate_limit=exchange_config.enable_rate_limit,
            timeout_ms=exchange_config.timeout_ms,
        )
    exchange = create_exchange(exchange_config)
    return list_markets_from_exchange(exchange=exchange, exchange_name=exchange_name)


def write_frame(frame: pd.DataFrame, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path
