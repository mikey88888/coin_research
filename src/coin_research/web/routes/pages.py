from __future__ import annotations

from urllib.parse import parse_qs

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from ...services.backtest_runs import build_leaderboard_context, build_run_detail_context, build_runs_index_context, build_strategy_compare_context
from ...services.market_views import build_asset_detail_context, build_market_home_context, build_symbol_list_context
from ...live.connectivity import BinanceConnectivityError
from ...services.paper import build_paper_dashboard_context, start_paper_session, stop_paper_session
from ..templating import TEMPLATES

router = APIRouter()


def _render(request: Request, *, template_name: str, context: dict) -> HTMLResponse:
    return TEMPLATES.TemplateResponse(request, template_name, {"request": request, **context})


@router.get("/", response_class=HTMLResponse)
@router.get("/markets", response_class=HTMLResponse)
def market_home(request: Request) -> HTMLResponse:
    return _render(request, template_name="pages/market_home.html", context=build_market_home_context())


@router.get("/markets/crypto", response_class=HTMLResponse)
def symbol_list(request: Request, q: str | None = None) -> HTMLResponse:
    return _render(request, template_name="pages/symbol_list.html", context=build_symbol_list_context(q=q))


@router.get("/markets/crypto/{symbol:path}", response_class=HTMLResponse)
def symbol_detail(
    request: Request,
    symbol: str,
    timeframe: str = Query("1d"),
    run_id: str | None = Query(None),
    trade_id: str | None = Query(None),
) -> HTMLResponse:
    try:
        context = build_asset_detail_context(symbol, timeframe=timeframe, run_id=run_id, trade_id=trade_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _render(request, template_name="pages/asset_detail.html", context=context)


@router.get("/research/runs", response_class=HTMLResponse)
def research_runs(request: Request) -> HTMLResponse:
    return _render(request, template_name="pages/research_runs.html", context=build_runs_index_context())


@router.get("/research/runs/{run_id}", response_class=HTMLResponse)
def research_run_detail(request: Request, run_id: str) -> HTMLResponse:
    try:
        context = build_run_detail_context(run_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _render(request, template_name="pages/research_run_detail.html", context=context)


@router.get("/research/leaderboard", response_class=HTMLResponse)
def research_leaderboard(request: Request) -> HTMLResponse:
    return _render(request, template_name="pages/research_leaderboard.html", context=build_leaderboard_context())


@router.get("/research/strategies/{strategy_key}", response_class=HTMLResponse)
def research_strategy_compare(request: Request, strategy_key: str) -> HTMLResponse:
    return _render(request, template_name="pages/research_strategy_compare.html", context=build_strategy_compare_context(strategy_key))


@router.get("/paper", response_class=HTMLResponse)
def paper_dashboard(request: Request) -> HTMLResponse:
    return _render(request, template_name="pages/paper_dashboard.html", context=build_paper_dashboard_context())


def _first_form_value(payload: dict[str, list[str]], key: str, *, default: str = "") -> str:
    values = payload.get(key) or [default]
    return values[0]


def _positive_int_form_value(payload: dict[str, list[str]], key: str, *, default: str) -> int:
    raw = _first_form_value(payload, key, default=default).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be a positive integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{key} must be a positive integer, got {value}")
    return value


def _positive_float_form_value(payload: dict[str, list[str]], key: str, *, default: str) -> float:
    raw = _first_form_value(payload, key, default=default).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be a positive number, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{key} must be a positive number, got {value}")
    return value


@router.post("/paper/start", response_class=HTMLResponse)
async def paper_start(request: Request):
    try:
        body = await request.body()
        form = parse_qs(body.decode("utf-8"), keep_blank_values=True)
        timeframe = _first_form_value(form, "timeframe", default="30m")
        top_n = _positive_int_form_value(form, "top_n", default="20")
        initial_capital = _positive_float_form_value(form, "initial_capital", default="100000")
        start_paper_session(timeframe=timeframe, top_n=top_n, initial_capital=initial_capital)
    except BinanceConnectivityError as exc:
        context = build_paper_dashboard_context(action_error=str(exc), connectivity_report=exc.report)
        return _render(request, template_name="pages/paper_dashboard.html", context=context)
    except Exception as exc:
        context = build_paper_dashboard_context(action_error=str(exc))
        return _render(request, template_name="pages/paper_dashboard.html", context=context)
    return RedirectResponse(url="/paper", status_code=303)


@router.post("/paper/stop", response_class=HTMLResponse)
async def paper_stop(request: Request):
    try:
        stop_paper_session()
    except Exception as exc:
        context = build_paper_dashboard_context(action_error=str(exc))
        return _render(request, template_name="pages/paper_dashboard.html", context=context)
    return RedirectResponse(url="/paper", status_code=303)
