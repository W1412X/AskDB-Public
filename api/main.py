"""
FastAPI app for AskDB Web UI: config, init, query.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config_router import router as config_router
from api.init_router import router as init_router
from api.query_router import router as query_router


def create_app() -> FastAPI:
    app = FastAPI(title="AskDB API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(config_router)
    app.include_router(init_router)
    app.include_router(query_router)
    return app


app = create_app()


@app.on_event("startup")
def startup() -> None:
    from utils.data_paths import DataPaths
    DataPaths.default().ensure_base_dirs()
