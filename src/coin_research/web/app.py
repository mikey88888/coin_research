from __future__ import annotations

import argparse

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .routes.pages import router as pages_router
from .templating import STATIC_ROOT


def create_app() -> FastAPI:
    app = FastAPI(title="Crypto Research Dashboard")
    app.mount("/static", StaticFiles(directory=str(STATIC_ROOT)), name="static")
    app.include_router(pages_router)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the crypto research dashboard")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8001)
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
