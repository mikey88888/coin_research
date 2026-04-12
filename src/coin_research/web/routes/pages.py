from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from ...services.backtest_runs import build_run_detail_context, build_runs_index_context, build_strategy_compare_context
from ...services.market_views import build_asset_detail_context, build_market_home_context, build_symbol_list_context
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


@router.get("/research/strategies/{strategy_key}", response_class=HTMLResponse)
def research_strategy_compare(request: Request, strategy_key: str) -> HTMLResponse:
    return _render(request, template_name="pages/research_strategy_compare.html", context=build_strategy_compare_context(strategy_key))
