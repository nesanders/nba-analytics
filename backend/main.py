"""
FastAPI application entry point.

Configures CORS (locked to ALLOWED_ORIGIN env var), attaches the slowapi rate
limiter, registers the /chat and /shot_chart routers, and initialises the DuckDB
connection at startup via the lifespan context.

Rate limits (per IP, per Cloud Run instance, configurable via env vars):
  RATE_LIMIT_CHAT  — default 20/minute  (/chat)
  RATE_LIMIT_SHOT  — default 10/minute  (/shot_chart)
Note: limits are in-memory and reset on cold start. With max-instances=2 the
effective burst ceiling is 2× these values.
"""
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi.errors import RateLimitExceeded

load_dotenv()

from chat import router as chat_router
from db import init_db
from limiter import limiter
from shot_chart import router as shot_chart_router

ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN", "http://localhost:5173")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="NBA Analytics API", lifespan=lifespan)
app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(_request: Request, _exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded. Please wait before sending another request."},
    )


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
