from __future__ import annotations

from dataclasses import asdict, dataclass
import os
import socket
import subprocess
import time
from typing import Any
from urllib.parse import urlparse

import requests

from ..config import ExchangeConfig
from ..exchanges import create_exchange
from ..time_utils import beijing_now


BINANCE_PING_URL = "https://api.binance.com/api/v3/ping"
GITHUB_CONTROL_URL = "https://api.github.com/"
PROXY_ENV_KEYS = ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy")


@dataclass(frozen=True)
class ConnectivityProbe:
    name: str
    target: str
    ok: bool
    elapsed_ms: int
    status_code: int | None = None
    error: str | None = None
    detail: str | None = None

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


class BinanceConnectivityError(RuntimeError):
    def __init__(self, report: dict[str, Any]):
        self.report = report
        super().__init__(str(report.get("summary") or "Binance connectivity preflight failed"))


def proxy_env_summary() -> list[dict[str, str]]:
    return [{"key": key, "value": os.environ[key]} for key in PROXY_ENV_KEYS if os.environ.get(key)]


def _first_proxy_url() -> str | None:
    for key in PROXY_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            return value
    return None


def _parsed_proxy_host_port(proxy_url: str | None) -> tuple[str, int] | None:
    if not proxy_url:
        return None
    parsed = urlparse(proxy_url)
    if not parsed.hostname or parsed.port is None:
        return None
    return parsed.hostname, parsed.port


def _default_wsl_gateway() -> str | None:
    try:
        output = subprocess.check_output(["ip", "route", "show", "default"], text=True, timeout=2)
    except Exception:
        return None
    parts = output.split()
    if "via" not in parts:
        return None
    index = parts.index("via")
    if index + 1 >= len(parts):
        return None
    return parts[index + 1]


def _gateway_proxy_url(proxy_url: str | None, gateway: str | None) -> str | None:
    parsed = urlparse(proxy_url or "")
    if not gateway or parsed.port is None or parsed.hostname not in {"127.0.0.1", "localhost"}:
        return None
    scheme = parsed.scheme or "http"
    return f"{scheme}://{gateway}:{parsed.port}"


def _tcp_probe(name: str, host: str, port: int, *, timeout_seconds: float) -> ConnectivityProbe:
    started = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return ConnectivityProbe(name=name, target=f"{host}:{port}", ok=True, elapsed_ms=elapsed_ms, detail="tcp connect ok")
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return ConnectivityProbe(name=name, target=f"{host}:{port}", ok=False, elapsed_ms=elapsed_ms, error=_short_error(exc))


def _http_probe(
    name: str,
    url: str,
    *,
    timeout_seconds: float,
    trust_env: bool,
    explicit_proxy: str | None = None,
) -> ConnectivityProbe:
    started = time.perf_counter()
    session = requests.Session()
    session.trust_env = trust_env
    proxies = {"http": explicit_proxy, "https": explicit_proxy} if explicit_proxy else None
    try:
        response = session.get(url, timeout=(2.0, timeout_seconds), proxies=proxies)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return ConnectivityProbe(
            name=name,
            target=url if explicit_proxy is None else f"{url} via {explicit_proxy}",
            ok=200 <= response.status_code < 300,
            elapsed_ms=elapsed_ms,
            status_code=response.status_code,
            detail=f"http status {response.status_code}",
        )
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return ConnectivityProbe(
            name=name,
            target=url if explicit_proxy is None else f"{url} via {explicit_proxy}",
            ok=False,
            elapsed_ms=elapsed_ms,
            error=_short_error(exc),
        )
    finally:
        session.close()


def _ccxt_exchange_info_probe(*, exchange_name: str, timeout_seconds: float) -> ConnectivityProbe:
    started = time.perf_counter()
    try:
        exchange = create_exchange(ExchangeConfig(exchange=exchange_name, timeout_ms=int(timeout_seconds * 1000)))
        exchange.publicGetExchangeInfo()
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return ConnectivityProbe(
            name="ccxt_exchange_info_env",
            target="https://api.binance.com/api/v3/exchangeInfo via ccxt",
            ok=True,
            elapsed_ms=elapsed_ms,
            detail="ccxt exchangeInfo ok",
        )
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return ConnectivityProbe(
            name="ccxt_exchange_info_env",
            target="https://api.binance.com/api/v3/exchangeInfo via ccxt",
            ok=False,
            elapsed_ms=elapsed_ms,
            error=_short_error(exc),
        )


def _short_error(exc: Exception) -> str:
    text = " ".join(str(exc).split())
    if not text:
        text = exc.__class__.__name__
    return text[:500]


def _probe_map(probes: list[ConnectivityProbe]) -> dict[str, ConnectivityProbe]:
    return {probe.name: probe for probe in probes}


def _summarize(probes: list[ConnectivityProbe], *, proxy_url: str | None) -> tuple[bool, str, str]:
    by_name = _probe_map(probes)
    direct_control = by_name.get("direct_internet_control")
    env_control = by_name.get("env_proxy_internet_control")
    direct_binance = by_name.get("direct_binance_ping")
    env_binance = by_name.get("env_binance_ping")
    ccxt_exchange_info = by_name.get("ccxt_exchange_info_env")

    if ccxt_exchange_info and ccxt_exchange_info.ok:
        return True, "Binance connectivity preflight passed: ccxt can reach exchangeInfo.", "可以启动模拟盘。"

    if proxy_url and direct_control and direct_control.ok and env_control and not env_control.ok:
        return (
            False,
            "Binance connectivity preflight failed: current HTTP(S)_PROXY is not usable from WSL, while direct internet works.",
            "先修代理环境变量或代理进程。当前 Python/ccxt 会继承 HTTP(S)_PROXY；如果代理 CONNECT 挂住，模拟盘必然启动失败。",
        )

    if direct_control and not direct_control.ok:
        return (
            False,
            "Binance connectivity preflight failed: WSL cannot reach the general internet control endpoint.",
            "先修 WSL 网络/DNS/防火墙，再启动模拟盘。",
        )

    if direct_binance and not direct_binance.ok and (not env_binance or not env_binance.ok):
        return (
            False,
            "Binance connectivity preflight failed: general internet works, but Binance REST is unreachable on all tested paths.",
            "需要提供一条能访问 api.binance.com 的网络路径；可修好代理，或确认当前网络/地区没有阻断 Binance。",
        )

    return (
        False,
        "Binance connectivity preflight failed: ccxt could not reach exchangeInfo.",
        "查看下方探针结果，优先修复失败的代理或 Binance REST 路径。",
    )


def diagnose_binance_connectivity(
    *,
    exchange_name: str = "binance",
    timeout_seconds: float = 5.0,
    include_ccxt: bool = True,
) -> dict[str, Any]:
    proxy_url = _first_proxy_url()
    proxy_host_port = _parsed_proxy_host_port(proxy_url)
    gateway = _default_wsl_gateway()
    gateway_proxy = _gateway_proxy_url(proxy_url, gateway)
    probes: list[ConnectivityProbe] = []

    if proxy_host_port:
        probes.append(
            _tcp_probe(
                "env_proxy_tcp",
                proxy_host_port[0],
                proxy_host_port[1],
                timeout_seconds=min(timeout_seconds, 2.0),
            )
        )

    probes.append(
        _http_probe(
            "direct_internet_control",
            GITHUB_CONTROL_URL,
            timeout_seconds=timeout_seconds,
            trust_env=False,
        )
    )
    if proxy_url:
        probes.append(
            _http_probe(
                "env_proxy_internet_control",
                GITHUB_CONTROL_URL,
                timeout_seconds=timeout_seconds,
                trust_env=True,
            )
        )
    probes.append(
        _http_probe(
            "direct_binance_ping",
            BINANCE_PING_URL,
            timeout_seconds=timeout_seconds,
            trust_env=False,
        )
    )
    probes.append(
        _http_probe(
            "env_binance_ping",
            BINANCE_PING_URL,
            timeout_seconds=timeout_seconds,
            trust_env=True,
        )
    )
    if gateway_proxy:
        gateway_host_port = _parsed_proxy_host_port(gateway_proxy)
        if gateway_host_port:
            probes.append(
                _tcp_probe(
                    "wsl_gateway_proxy_tcp",
                    gateway_host_port[0],
                    gateway_host_port[1],
                    timeout_seconds=min(timeout_seconds, 2.0),
                )
            )
        probes.append(
            _http_probe(
                "wsl_gateway_proxy_binance_ping",
                BINANCE_PING_URL,
                timeout_seconds=timeout_seconds,
                trust_env=False,
                explicit_proxy=gateway_proxy,
            )
        )

    env_binance = _probe_map(probes).get("env_binance_ping")
    if include_ccxt and env_binance and env_binance.ok:
        probes.append(_ccxt_exchange_info_probe(exchange_name=exchange_name, timeout_seconds=timeout_seconds))

    ok, summary, recommendation = _summarize(probes, proxy_url=proxy_url)
    return {
        "ok": ok,
        "generated_at": beijing_now().isoformat(),
        "exchange": exchange_name,
        "proxy_env": proxy_env_summary(),
        "wsl_gateway": gateway,
        "gateway_proxy": gateway_proxy,
        "summary": summary,
        "recommendation": recommendation,
        "probes": [probe.to_record() for probe in probes],
    }


def format_connectivity_report(report: dict[str, Any]) -> str:
    lines = [
        f"ok={str(report.get('ok')).lower()}",
        f"summary={report.get('summary')}",
        f"recommendation={report.get('recommendation')}",
    ]
    proxy_env = report.get("proxy_env") or []
    if proxy_env:
        lines.append("proxy_env=" + "; ".join(f"{item['key']}={item['value']}" for item in proxy_env))
    else:
        lines.append("proxy_env=none")
    if report.get("wsl_gateway"):
        lines.append(f"wsl_gateway={report['wsl_gateway']}")
    for probe in report.get("probes", []):
        status = "ok" if probe.get("ok") else "fail"
        extra = probe.get("detail") or probe.get("error") or ""
        lines.append(
            f"probe={probe.get('name')} status={status} elapsed_ms={probe.get('elapsed_ms')} "
            f"target={probe.get('target')} {extra}".rstrip()
        )
    return "\n".join(lines)


def main() -> None:
    report = diagnose_binance_connectivity()
    print(format_connectivity_report(report))
    raise SystemExit(0 if report["ok"] else 2)


if __name__ == "__main__":
    main()
