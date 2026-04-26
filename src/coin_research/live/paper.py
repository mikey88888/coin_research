from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import math
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from ..data import timeframe_to_milliseconds
from ..config import project_root
from ..sync import resolve_top_market_cap_universe
from ..strategies.absolute_momentum_volatility_composite import (
    DEFAULT_LOOKBACK_BARS,
    DEFAULT_VOLATILITY_WINDOW,
)


STRATEGY_KEY = "absolute-momentum-volatility-composite"
DEFAULT_EXCHANGE = "binance"
DEFAULT_QUOTE = "USDT"
DEFAULT_TIMEFRAME = "30m"
DEFAULT_TOP_N = 20
V1_HOLD_BARS = 5
V1_TOP_K = 5
V1_REBALANCE_INTERVAL = 5
V1_MIN_VOLATILITY_PCT = 0.5
V1_MIN_MOMENTUM_PCT = 5.0
STALE_HEARTBEAT_SECONDS = 600
SESSION_ACTIVE_STATUSES = {"created", "running", "stop_requested"}
TIMEFRAME_CHOICES = ("30m", "4h", "1d")


@dataclass(frozen=True)
class PaperTradingConfig:
    exchange: str = DEFAULT_EXCHANGE
    quote: str = DEFAULT_QUOTE
    timeframe: str = DEFAULT_TIMEFRAME
    initial_capital: float = 100000.0
    top_n: int = DEFAULT_TOP_N
    position_target_pct: float = 0.2
    max_positions: int = 5
    max_gross_exposure_pct: float = 1.0
    fee_rate: float = 0.001
    quantity_step: float = 0.0001
    lookback_bars: int = 60
    volatility_window: int = 60
    hold_bars: int = V1_HOLD_BARS
    top_k: int = V1_TOP_K
    rebalance_interval: int = V1_REBALANCE_INTERVAL
    min_volatility_pct: float = V1_MIN_VOLATILITY_PCT
    min_momentum_pct: float = V1_MIN_MOMENTUM_PCT

    def to_record(self) -> dict[str, Any]:
        return asdict(self)

    def validate(self) -> PaperTradingConfig:
        if self.exchange != DEFAULT_EXCHANGE:
            raise ValueError(f"paper trading currently supports exchange={DEFAULT_EXCHANGE} only, got {self.exchange}")
        if self.quote != DEFAULT_QUOTE:
            raise ValueError(f"paper trading currently supports quote={DEFAULT_QUOTE} only, got {self.quote}")
        if self.timeframe not in TIMEFRAME_CHOICES:
            raise ValueError(f"unsupported timeframe: {self.timeframe}")
        if self.initial_capital <= 0:
            raise ValueError(f"initial_capital must be > 0, got {self.initial_capital}")
        if self.top_n <= 0:
            raise ValueError(f"top_n must be > 0, got {self.top_n}")
        if self.position_target_pct <= 0:
            raise ValueError(f"position_target_pct must be > 0, got {self.position_target_pct}")
        if self.max_positions <= 0:
            raise ValueError(f"max_positions must be > 0, got {self.max_positions}")
        if self.max_gross_exposure_pct <= 0:
            raise ValueError(f"max_gross_exposure_pct must be > 0, got {self.max_gross_exposure_pct}")
        if self.fee_rate < 0:
            raise ValueError(f"fee_rate must be >= 0, got {self.fee_rate}")
        if self.quantity_step <= 0:
            raise ValueError(f"quantity_step must be > 0, got {self.quantity_step}")
        if self.lookback_bars < DEFAULT_LOOKBACK_BARS:
            raise ValueError(f"lookback_bars must be >= {DEFAULT_LOOKBACK_BARS}, got {self.lookback_bars}")
        if self.volatility_window < DEFAULT_VOLATILITY_WINDOW:
            raise ValueError(f"volatility_window must be >= {DEFAULT_VOLATILITY_WINDOW}, got {self.volatility_window}")
        if self.hold_bars != V1_HOLD_BARS:
            raise ValueError(f"paper trading V1 only supports hold_bars={V1_HOLD_BARS}")
        if self.top_k != V1_TOP_K:
            raise ValueError(f"paper trading V1 only supports top_k={V1_TOP_K}")
        if self.rebalance_interval != V1_REBALANCE_INTERVAL:
            raise ValueError(f"paper trading V1 only supports rebalance_interval={V1_REBALANCE_INTERVAL}")
        if self.min_volatility_pct != V1_MIN_VOLATILITY_PCT:
            raise ValueError(f"paper trading V1 only supports min_volatility_pct={V1_MIN_VOLATILITY_PCT}")
        if self.min_momentum_pct != V1_MIN_MOMENTUM_PCT:
            raise ValueError(f"paper trading V1 only supports min_momentum_pct={V1_MIN_MOMENTUM_PCT}")
        return self


@dataclass(frozen=True)
class MarketRule:
    quantity_step: float
    min_notional: float


@dataclass(frozen=True)
class PaperSignal:
    signal_id: str
    symbol: str
    signal_time: pd.Timestamp
    entry_time: pd.Timestamp
    planned_exit_time: pd.Timestamp
    entry_price: float
    momentum_pct: float
    volatility_pct: float
    score: float
    rank: int


@dataclass(frozen=True)
class PaperPosition:
    symbol: str
    signal_id: str
    entry_time: pd.Timestamp
    planned_exit_time: pd.Timestamp
    quantity: float
    entry_price: float
    entry_fee: float
    entry_notional: float

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PaperOrder:
    timestamp: pd.Timestamp
    symbol: str
    side: str
    price: float
    quantity: float
    turnover: float
    fee: float
    slippage: float
    reason: str
    signal_id: str = ""

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PaperEquityPoint:
    timestamp: pd.Timestamp
    cash: float
    market_value: float
    equity: float
    gross_exposure_pct: float
    drawdown_pct: float
    position_count: int

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PaperExecutionResult:
    cash: float
    peak_equity: float
    positions: dict[str, PaperPosition]
    orders: list[PaperOrder]
    equity_point: PaperEquityPoint


def build_default_config(*, timeframe: str = DEFAULT_TIMEFRAME, initial_capital: float = 100000.0, top_n: int = DEFAULT_TOP_N) -> PaperTradingConfig:
    return PaperTradingConfig(
        timeframe=timeframe,
        initial_capital=initial_capital,
        top_n=top_n,
        lookback_bars=DEFAULT_LOOKBACK_BARS,
        volatility_window=DEFAULT_VOLATILITY_WINDOW,
        hold_bars=V1_HOLD_BARS,
        top_k=V1_TOP_K,
        rebalance_interval=V1_REBALANCE_INTERVAL,
        min_volatility_pct=V1_MIN_VOLATILITY_PCT,
        min_momentum_pct=V1_MIN_MOMENTUM_PCT,
    ).validate()


def generate_session_id() -> str:
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")
    return f"paper-{timestamp}-{uuid4().hex[:8]}"


def paper_log_dir(root: Path | None = None) -> Path:
    return (root or project_root()) / "log" / "paper_trading"


def paper_log_path(session_id: str, *, root: Path | None = None) -> Path:
    return paper_log_dir(root) / f"{session_id}.log"


def signal_interval_delta(timeframe: str, *, bars: int) -> timedelta:
    return timedelta(milliseconds=timeframe_to_milliseconds(timeframe) * bars)


def compute_latest_signal_time(now: datetime, *, timeframe: str) -> pd.Timestamp | None:
    current_open_ms = int(now.timestamp() * 1000) // timeframe_to_milliseconds(timeframe) * timeframe_to_milliseconds(timeframe)
    latest_signal_ms = current_open_ms - timeframe_to_milliseconds(timeframe)
    if latest_signal_ms < 0:
        return None
    return pd.Timestamp(latest_signal_ms, unit="ms", tz="UTC")


def is_process_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def is_session_stale(session: dict[str, Any] | None, *, now: datetime | None = None) -> bool:
    if not session:
        return False
    if str(session.get("status")) not in SESSION_ACTIVE_STATUSES:
        return False
    heartbeat = pd.to_datetime(session.get("heartbeat_at"), errors="coerce", utc=True)
    if pd.isna(heartbeat):
        return not is_process_alive(_as_int(session.get("pid")))
    current = pd.Timestamp(now or datetime.now(tz=UTC))
    if (current - heartbeat).total_seconds() <= STALE_HEARTBEAT_SECONDS:
        return False
    return not is_process_alive(_as_int(session.get("pid")))


def snapshot_universe_symbols(*, exchange, exchange_name: str, top_n: int, quote: str) -> list[str]:
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
    markets_frame = pd.DataFrame(rows)
    universe = resolve_top_market_cap_universe(
        exchange_name=exchange_name,
        markets_frame=markets_frame,
        top_n=top_n,
        quote=quote,
    )
    if universe.empty:
        return []
    return universe["market_symbol"].astype(str).tolist()


def build_market_rules(*, exchange, symbols: list[str], default_step: float) -> dict[str, MarketRule]:
    exchange.load_markets()
    market_map = getattr(exchange, "markets", {}) or {}
    rules: dict[str, MarketRule] = {}
    for symbol in symbols:
        payload = market_map.get(symbol, {})
        quantity_step = default_step
        min_notional = 0.0
        precision_amount = payload.get("precision", {}).get("amount") if isinstance(payload.get("precision"), dict) else None
        if isinstance(precision_amount, int) and precision_amount >= 0:
            quantity_step = 10 ** (-precision_amount)
        filters = payload.get("info", {}).get("filters", []) if isinstance(payload.get("info"), dict) else []
        for item in filters:
            if not isinstance(item, dict):
                continue
            if item.get("filterType") == "LOT_SIZE":
                step_size = _as_float(item.get("stepSize"))
                if step_size and step_size > 0:
                    quantity_step = step_size
            if item.get("filterType") in {"MIN_NOTIONAL", "NOTIONAL"}:
                notional = _as_float(item.get("minNotional"))
                if notional and notional > 0:
                    min_notional = notional
        rules[symbol] = MarketRule(quantity_step=quantity_step, min_notional=min_notional)
    return rules


def select_signals_for_time(
    market_frames: dict[str, pd.DataFrame],
    *,
    signal_time: pd.Timestamp,
    config: PaperTradingConfig,
    time_column: str = "bar_time",
) -> list[PaperSignal]:
    config.validate()
    prepared = {symbol: _prepare_frame(frame, time_column=time_column) for symbol, frame in market_frames.items()}
    prepared = {symbol: frame for symbol, frame in prepared.items() if not frame.empty}
    if not prepared:
        return []

    close_wide = pd.concat([frame.set_index(time_column)["close"].rename(symbol) for symbol, frame in prepared.items()], axis=1).sort_index()
    open_wide = pd.concat([frame.set_index(time_column)["open"].rename(symbol) for symbol, frame in prepared.items()], axis=1).sort_index()
    timeline = list(close_wide.index)
    if signal_time not in close_wide.index:
        return []

    signal_index = timeline.index(signal_time)
    required_history = max(config.lookback_bars, config.volatility_window)
    if signal_index < required_history or signal_index + 1 >= len(timeline):
        return []

    entry_index = signal_index + 1
    entry_time = timeline[entry_index]
    ranking_close = close_wide.iloc[signal_index]
    lookback_close = close_wide.iloc[signal_index - config.lookback_bars]
    returns_wide = close_wide.pct_change() * 100.0
    realized_volatility = returns_wide.rolling(config.volatility_window).std().iloc[signal_index]
    ranking = pd.DataFrame(
        {
            "momentum_pct": ((ranking_close / lookback_close) - 1.0) * 100.0,
            "volatility_pct": realized_volatility,
        }
    )
    ranking = ranking.replace([pd.NA, pd.NaT, float("inf"), float("-inf")], pd.NA).dropna()
    ranking = ranking[ranking["momentum_pct"] >= config.min_momentum_pct]
    if len(ranking) < max(config.top_k, config.top_k):
        return []
    ranking["volatility_pct"] = ranking["volatility_pct"].clip(lower=config.min_volatility_pct)
    ranking["score"] = ranking["momentum_pct"] / ranking["volatility_pct"]
    selected = ranking.sort_values(["score", "momentum_pct"], ascending=False).head(config.top_k)
    planned_exit_time = entry_time + signal_interval_delta(config.timeframe, bars=config.hold_bars)

    signals: list[PaperSignal] = []
    for rank, (symbol, row) in enumerate(selected.iterrows(), start=1):
        entry_price = open_wide.iloc[entry_index].get(symbol)
        if pd.isna(entry_price) or float(entry_price) <= 0:
            continue
        signals.append(
            PaperSignal(
                signal_id=f"{symbol}-{signal_time.strftime('%Y%m%dT%H%M%S')}-paper-r{rank}",
                symbol=symbol,
                signal_time=pd.Timestamp(signal_time),
                entry_time=pd.Timestamp(entry_time),
                planned_exit_time=pd.Timestamp(planned_exit_time),
                entry_price=round(float(entry_price), 8),
                momentum_pct=round(float(row["momentum_pct"]), 6),
                volatility_pct=round(float(row["volatility_pct"]), 6),
                score=round(float(row["score"]), 6),
                rank=rank,
            )
        )
    return signals


def execution_prices_for_time(market_frames: dict[str, pd.DataFrame], *, execution_time: pd.Timestamp, time_column: str = "bar_time") -> dict[str, float]:
    prices: dict[str, float] = {}
    for symbol, frame in market_frames.items():
        prepared = _prepare_frame(frame, time_column=time_column)
        if prepared.empty:
            continue
        matched = prepared[prepared[time_column] == execution_time]
        if matched.empty:
            continue
        price = _as_float(matched.iloc[0]["open"])
        if price is not None and price > 0:
            prices[symbol] = price
    return prices


def apply_execution(
    *,
    execution_time: pd.Timestamp,
    session_id: str,
    positions: dict[str, PaperPosition],
    signals: list[PaperSignal],
    open_prices: dict[str, float],
    cash: float,
    peak_equity: float,
    config: PaperTradingConfig,
    market_rules: dict[str, MarketRule],
) -> PaperExecutionResult:
    current_positions = dict(positions)
    current_cash = float(cash)
    orders: list[PaperOrder] = []

    exiting_symbols = [symbol for symbol, position in current_positions.items() if position.planned_exit_time <= execution_time]
    for symbol in sorted(exiting_symbols):
        position = current_positions.get(symbol)
        if position is None:
            continue
        price = open_prices.get(symbol)
        if price is None or price <= 0:
            continue
        turnover = position.quantity * price
        fee = turnover * config.fee_rate
        current_cash += turnover - fee
        orders.append(
            PaperOrder(
                timestamp=execution_time,
                symbol=symbol,
                side="sell",
                price=round(float(price), 8),
                quantity=round(float(position.quantity), 8),
                turnover=round(float(turnover), 8),
                fee=round(float(fee), 8),
                slippage=0.0,
                reason="rebalance_exit",
                signal_id=position.signal_id,
            )
        )
        current_positions.pop(symbol, None)

    for signal in sorted(signals, key=lambda item: (item.rank, item.symbol)):
        if signal.symbol in current_positions:
            continue
        if len(current_positions) >= config.max_positions:
            continue
        price = open_prices.get(signal.symbol)
        if price is None or price <= 0:
            continue
        equity_before_entry, market_value_before_entry = _compute_equity(current_cash, current_positions, open_prices)
        target_notional = equity_before_entry * config.position_target_pct
        remaining_exposure = max(equity_before_entry * config.max_gross_exposure_pct - market_value_before_entry, 0.0)
        budget = min(target_notional, remaining_exposure, current_cash)
        rule = market_rules.get(signal.symbol, MarketRule(quantity_step=config.quantity_step, min_notional=0.0))
        quantity = _execution_quantity(
            budget=budget,
            execution_price=price,
            fee_rate=config.fee_rate,
            quantity_step=rule.quantity_step,
        )
        if quantity <= 0:
            continue
        turnover = quantity * price
        if rule.min_notional > 0 and turnover < rule.min_notional:
            continue
        fee = turnover * config.fee_rate
        total_cost = turnover + fee
        if total_cost > current_cash:
            continue
        current_cash -= total_cost
        current_positions[signal.symbol] = PaperPosition(
            symbol=signal.symbol,
            signal_id=signal.signal_id,
            entry_time=execution_time,
            planned_exit_time=signal.planned_exit_time,
            quantity=quantity,
            entry_price=price,
            entry_fee=fee,
            entry_notional=turnover,
        )
        orders.append(
            PaperOrder(
                timestamp=execution_time,
                symbol=signal.symbol,
                side="buy",
                price=round(float(price), 8),
                quantity=round(float(quantity), 8),
                turnover=round(float(turnover), 8),
                fee=round(float(fee), 8),
                slippage=0.0,
                reason="entry",
                signal_id=signal.signal_id,
            )
        )

    equity, market_value = _compute_equity(current_cash, current_positions, open_prices)
    updated_peak = max(float(peak_equity), float(equity))
    drawdown_pct = ((updated_peak - equity) / updated_peak) * 100.0 if updated_peak else 0.0
    gross_exposure_pct = (market_value / equity) * 100.0 if equity else 0.0
    equity_point = PaperEquityPoint(
        timestamp=execution_time,
        cash=round(float(current_cash), 8),
        market_value=round(float(market_value), 8),
        equity=round(float(equity), 8),
        gross_exposure_pct=round(float(gross_exposure_pct), 6),
        drawdown_pct=round(float(drawdown_pct), 6),
        position_count=len(current_positions),
    )
    return PaperExecutionResult(
        cash=round(float(current_cash), 8),
        peak_equity=round(float(updated_peak), 8),
        positions=current_positions,
        orders=orders,
        equity_point=equity_point,
    )


def _prepare_frame(df: pd.DataFrame, *, time_column: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    required = {time_column, "open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")
    frame = df.loc[:, [column for column in df.columns if column in required or column in {"symbol", "volume"}]].copy()
    frame[time_column] = pd.to_datetime(frame[time_column], errors="coerce", utc=True)
    for column in ("open", "high", "low", "close"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame[(frame["open"] > 0) & (frame["high"] > 0) & (frame["low"] > 0) & (frame["close"] > 0)]
    return frame.dropna(subset=[time_column, "open", "high", "low", "close"]).sort_values(time_column).reset_index(drop=True)


def _compute_equity(cash: float, positions: dict[str, PaperPosition], marks: dict[str, float]) -> tuple[float, float]:
    market_value = 0.0
    for symbol, position in positions.items():
        mark = marks.get(symbol, position.entry_price)
        market_value += position.quantity * mark
    return cash + market_value, market_value


def _execution_quantity(*, budget: float, execution_price: float, fee_rate: float, quantity_step: float) -> float:
    if execution_price <= 0 or budget <= 0 or quantity_step <= 0:
        return 0.0
    gross_per_unit = execution_price * (1.0 + fee_rate)
    raw_quantity = budget / gross_per_unit
    units = math.floor(raw_quantity / quantity_step)
    return round(units * quantity_step, 8)


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _as_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


__all__ = [
    "DEFAULT_EXCHANGE",
    "DEFAULT_QUOTE",
    "DEFAULT_TIMEFRAME",
    "DEFAULT_TOP_N",
    "V1_HOLD_BARS",
    "V1_TOP_K",
    "V1_REBALANCE_INTERVAL",
    "V1_MIN_VOLATILITY_PCT",
    "V1_MIN_MOMENTUM_PCT",
    "PaperEquityPoint",
    "PaperExecutionResult",
    "PaperOrder",
    "PaperPosition",
    "PaperSignal",
    "PaperTradingConfig",
    "SESSION_ACTIVE_STATUSES",
    "STALE_HEARTBEAT_SECONDS",
    "STRATEGY_KEY",
    "TIMEFRAME_CHOICES",
    "apply_execution",
    "build_default_config",
    "build_market_rules",
    "compute_latest_signal_time",
    "execution_prices_for_time",
    "generate_session_id",
    "is_process_alive",
    "is_session_stale",
    "select_signals_for_time",
    "signal_interval_delta",
    "snapshot_universe_symbols",
]
