"""
Carbon Footprint Analysis Backend — Entry Point
================================================
FastAPI application that receives PDF documents, orchestrates
three Gemini-powered AI agents (OCR → Carbon Calculator → Auditor),
and returns structured carbon-footprint analysis results.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.settings import settings
from api.routes import router as api_router
from db.snowflake_client import init_tables


def create_app() -> FastAPI:
    """Application factory."""
    app = FastAPI(
        title="Carbon Footprint Analyzer",
        description="AI-powered supply-chain carbon footprint analysis",
        version="0.1.0",
    )

    # ── CORS (allow frontend origin) ──────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # tighten in production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Register routers ──────────────────────────────────
    app.include_router(api_router, prefix="/api")

    # ── Startup events ────────────────────────────────────
    @app.on_event("startup")
    async def on_startup():
        """Initialize database tables on first run."""
        init_tables()

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
