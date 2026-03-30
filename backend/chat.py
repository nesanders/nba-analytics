"""
POST /chat endpoint — the core request/response loop.

Two-stage LLM flow:
  Stage 1: Send user message + conversation history to the LLM.
           The model returns JSON with {sql, chart, text}. Only sql and chart
           are used from this response.
  Stage 2: Execute the SQL against DuckDB. Pass the top rows back to the LLM
           in a second short call to generate response text grounded in the
           actual data rather than training-time knowledge.

Two providers are supported, selected by request header:
  X-Groq-Key:     user-supplied Groq key  → llama-3.3-70b-versatile (free tier)
  X-Gemini-Token: server-side auth token  → gemini-2.5-flash (operator-paid)
"""
import json
import os
import re
from typing import Any

from fastapi import APIRouter, Header, Request
from pydantic import BaseModel

from chart_builder import build_figure
from db import run_query
from limiter import RATE_LIMIT_CHAT, limiter
from llm import get_llm_client
from schema_context import SYSTEM_PROMPT

router = APIRouter()

MAX_HISTORY_TURNS = 4   # keep last N user+assistant pairs (reduces token cost)
MAX_ROWS = 500          # hard cap on rows returned to chart builder
SUMMARIZE_ROWS = 15     # rows shown to LLM for text generation
MAX_SQL_RETRIES = 3     # max attempts to fix failing SQL before giving up


class ChatMessage(BaseModel):
    role: str   # "user" | "assistant"
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = []


class ChatResponse(BaseModel):
    text: str
    figure: dict | None = None
    sql: str | None = None
    error: str | None = None
    provider: str | None = None


@router.post("/chat", response_model=ChatResponse)
@limiter.limit(RATE_LIMIT_CHAT)
async def chat(
    request: Request,
    req: ChatRequest,
    x_groq_key: str | None = Header(default=None, alias="X-Groq-Key"),
    x_gemini_token: str | None = Header(default=None, alias="X-Gemini-Token"),
):
    client = get_llm_client(groq_key=x_groq_key, gemini_token=x_gemini_token)

    # --- Stage 1: Generate SQL + chart spec ---
    history = req.history[-(MAX_HISTORY_TURNS * 2):]
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in history:
        messages.append({"role": msg.role, "content": msg.content})
    messages.append({"role": "user", "content": req.message})

    raw = client.complete(messages, json_mode=True, max_tokens=1024)
    llm_response = _parse_llm_response(raw)
    fallback_text = llm_response.get("text", "")
    sql = llm_response.get("sql")
    chart_spec = llm_response.get("chart")

    # --- Execute SQL (with up to MAX_SQL_RETRIES attempts to fix errors) ---
    rows = []
    figure = None
    sql_error = None
    if sql:
        try:
            sql = _sanitize_sql(sql)
        except ValueError as e:
            sql_error = str(e)

        if not sql_error:
            for attempt in range(MAX_SQL_RETRIES):
                try:
                    rows = run_query(sql)
                    if len(rows) > MAX_ROWS:
                        rows = rows[:MAX_ROWS]
                    sql_error = None
                    break
                except Exception as e:
                    sql_error = str(e)
                    if attempt < MAX_SQL_RETRIES - 1:
                        fixed = _retry_fix_sql(client, req.message, sql, sql_error)
                        if fixed:
                            try:
                                sql = _sanitize_sql(fixed)
                            except ValueError:
                                break
                        else:
                            break

        # Build figure separately — chart errors must not trigger SQL retry
        if not sql_error and rows and chart_spec and chart_spec.get("type") and chart_spec["type"] != "null":
            try:
                figure = build_figure(rows, chart_spec)
            except Exception as e:
                figure = None  # chart build failed; SQL + text still returned

    # --- Stage 2: Generate text from actual query results ---
    if sql and not sql_error and rows:
        text = _generate_summary(client, req.message, rows)
    elif sql and sql_error:
        text = fallback_text + f"\n\n_(Query failed: {sql_error})_"
    else:
        text = fallback_text

    return ChatResponse(
        text=text,
        figure=figure,
        sql=sql,
        error=sql_error,
        provider=client.provider,
    )


def _round_row(row: dict) -> dict:
    """Round floats to 3 decimal places so the LLM doesn't quote 10-digit precision."""
    return {k: round(v, 3) if isinstance(v, float) else v for k, v in row.items()}


def _generate_summary(client, question: str, rows: list[dict]) -> str:
    """Second LLM call: generate text grounded in actual query results."""
    sample = [_round_row(r) for r in rows[:SUMMARIZE_ROWS]]
    results_text = json.dumps(sample, default=str, indent=None)
    prompt = (
        f"The user asked: {question}\n\n"
        f"The database returned these results (first {len(sample)} rows):\n{results_text}\n\n"
        "Write a concise 1-3 sentence answer grounded in these exact results. "
        "Name specific players/teams/numbers from the data. Do not guess or use outside knowledge. "
        "Round numbers to a natural precision (e.g. 24.3 ppg, not 24.312). "
        "Return plain text only, no JSON."
    )
    try:
        return client.complete(
            [{"role": "user", "content": prompt}],
            json_mode=False,
            max_tokens=512,
        ).strip()
    except Exception:
        top = rows[0]
        keys = list(top.keys())
        return f"Top result: {', '.join(f'{k}={top[k]}' for k in keys[:4])}"


def _retry_fix_sql(client, question: str, failed_sql: str, error: str) -> str | None:
    """Ask the LLM to fix a failing SQL query. Returns corrected SQL or None."""
    prompt = (
        f"The following DuckDB SQL query failed:\n\n{failed_sql}\n\n"
        f"Error: {error}\n\n"
        f"Original question: {question}\n\n"
        "Return ONLY the corrected SQL — no JSON, no explanation, no code fences."
    )
    try:
        fixed = client.complete(
            [{"role": "system", "content": SYSTEM_PROMPT},
             {"role": "user", "content": prompt}],
            json_mode=False,
            max_tokens=512,
        ).strip()
        fixed = re.sub(r"^```(?:sql)?\s*", "", fixed)
        fixed = re.sub(r"\s*```$", "", fixed)
        return fixed.strip() or None
    except Exception:
        return None


def _parse_llm_response(raw: str) -> dict[str, Any]:
    """Parse the LLM JSON response, with fallback for malformed output."""
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        text_match = re.search(r'"text"\s*:\s*"((?:[^"\\]|\\.)*)"', raw)
        return {"text": text_match.group(1) if text_match else raw, "sql": None, "chart": None}


def _sanitize_sql(sql: str) -> str:
    """Basic SQL safety check — reject write operations."""
    normalized = sql.strip().upper()
    forbidden = ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "REPLACE")
    for kw in forbidden:
        if re.search(rf"\b{kw}\b", normalized):
            raise ValueError(f"SQL contains forbidden keyword: {kw}")
    return sql
