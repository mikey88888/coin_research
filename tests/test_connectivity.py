from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from coin_research.live.connectivity import ConnectivityProbe, diagnose_binance_connectivity, format_connectivity_report


class BinanceConnectivityDiagnosticsTests(unittest.TestCase):
    def test_diagnose_identifies_broken_proxy_when_direct_internet_works(self) -> None:
        def fake_http_probe(name: str, url: str, **_kwargs):
            ok = name == "direct_internet_control"
            return ConnectivityProbe(name=name, target=url, ok=ok, elapsed_ms=10, error=None if ok else "timed out")

        with patch.dict(os.environ, {"HTTPS_PROXY": "http://127.0.0.1:7897"}, clear=True), patch(
            "coin_research.live.connectivity._default_wsl_gateway", return_value="172.20.160.1"
        ), patch("coin_research.live.connectivity._tcp_probe") as tcp_probe, patch(
            "coin_research.live.connectivity._http_probe", side_effect=fake_http_probe
        ):
            tcp_probe.side_effect = lambda name, host, port, **_kwargs: ConnectivityProbe(
                name=name, target=f"{host}:{port}", ok=True, elapsed_ms=1
            )
            report = diagnose_binance_connectivity(timeout_seconds=0.1, include_ccxt=True)

        self.assertFalse(report["ok"])
        self.assertIn("HTTP(S)_PROXY is not usable", report["summary"])
        self.assertTrue(any(probe["name"] == "env_proxy_tcp" for probe in report["probes"]))

    def test_diagnose_passes_only_after_ccxt_exchange_info_passes(self) -> None:
        with patch.dict(os.environ, {}, clear=True), patch(
            "coin_research.live.connectivity._default_wsl_gateway", return_value=None
        ), patch(
            "coin_research.live.connectivity._http_probe",
            side_effect=lambda name, url, **_kwargs: ConnectivityProbe(name=name, target=url, ok=True, elapsed_ms=10),
        ), patch(
            "coin_research.live.connectivity._ccxt_exchange_info_probe",
            return_value=ConnectivityProbe(
                name="ccxt_exchange_info_env",
                target="exchangeInfo",
                ok=True,
                elapsed_ms=10,
            ),
        ):
            report = diagnose_binance_connectivity(timeout_seconds=0.1, include_ccxt=True)

        self.assertTrue(report["ok"])
        self.assertIn("preflight passed", report["summary"])

    def test_format_report_includes_probe_details(self) -> None:
        report = {
            "ok": False,
            "summary": "failed",
            "recommendation": "fix network",
            "proxy_env": [{"key": "HTTPS_PROXY", "value": "http://127.0.0.1:7897"}],
            "wsl_gateway": "172.20.160.1",
            "probes": [
                {
                    "name": "env_binance_ping",
                    "target": "https://api.binance.com/api/v3/ping",
                    "ok": False,
                    "elapsed_ms": 100,
                    "error": "timed out",
                }
            ],
        }

        output = format_connectivity_report(report)

        self.assertIn("ok=false", output)
        self.assertIn("HTTPS_PROXY=http://127.0.0.1:7897", output)
        self.assertIn("probe=env_binance_ping status=fail", output)


if __name__ == "__main__":
    unittest.main()
