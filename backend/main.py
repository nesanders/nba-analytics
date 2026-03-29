"""
FastAPI application entry point.

Configures CORS (locked to ALLOWED_ORIGIN env var), registers the /chat and
/shot_chart routers, and initialises the DuckDB connection at startup via the
lifespan context. The /health endpoint is used by Cloud Run for readiness checks.
"""
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

from chat import router as chat_router
from shot_chart import router as shot_chart_router
from db import init_db

ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN", "http://localhost:5173")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="NBA Analytics API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN],
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)

app.include_router(chat_router)
app.include_router(shot_chart_router)


@app.get("/health")
def health():
    return {"status": "ok"}
