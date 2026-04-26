from __future__ import annotations

import argparse

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .routes.pages import router as pages_router
from .templating import STATIC_ROOT


def _nonblank_host(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise argparse.ArgumentTypeError("must not be blank")
    return normalized


def _positive_port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"must be an integer, got {value!r}") from exc
    if port <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive integer, got {value}")
    if port > 65535:
        raise argparse.ArgumentTypeError(f"must be <= 65535, got {value}")
    return port


def create_app() -> FastAPI:
    app = FastAPI(title="Crypto Research Dashboard")
    app.mount("/static", StaticFiles(directory=str(STATIC_ROOT)), name="static")
    app.include_router(pages_router)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the crypto research dashboard")
    parser.add_argument("--host", type=_nonblank_host, default="0.0.0.0")
    parser.add_argument("--port", type=_positive_port, default=8001)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    uvicorn.run(
        "coin_research.web.app:create_app",
        factory=True,
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
