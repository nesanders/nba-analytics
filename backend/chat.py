"""
POST /chat endpoint — the core request/response loop.

Two-stage LLM flow:
  Stage 1: Send user message + conversation history to Groq (llama-3.3-70b).
           The model returns JSON with {sql, chart, text}. Only sql and chart
           are used from this response.
  Stage 2: Execute the SQL against DuckDB. Pass the top rows back to the LLM
           in a second short call to generate response text grounded in the
           actual data rather than training-time knowledge.

The Groq API key is supplied by the user via the X-Groq-Key request header and
is never stored server-side.
"""
import json
import os
import re
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from groq import Groq
from pydantic import BaseModel

from chart_builder import build_figure
from db import run_query
from limiter import RATE_LIMIT_CHAT, limiter
from schema_context import SYSTEM_PROMPT

router = APIRouter()

MAX_HISTORY_TURNS = 10  # keep last N user+assistant pairs
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


@router.post("/chat", response_model=ChatResponse)
@limiter.limit(RATE_LIMIT_CHAT)
async def chat(
    request: Request,
    req: ChatRequest,
    x_groq_key: str = Header(..., alias="X-Groq-Key"),
):
    groq_client = Groq(api_key=x_groq_key)

    # --- Stage 1: Generate SQL + chart spec ---
    history = req.history[-(MAX_HISTORY_TURNS * 2):]
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in history:
        messages.append({"role": msg.role, "content": msg.content})
    messages.append({"role": "user", "content": req.message})

    try:
        completion = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=messages,
            temperature=0.1,
            max_tokens=1024,
            response_format={"type": "json_object"},
        )
    except Exception as e:
        err = str(e)
        if "401" in err or "invalid_api_key" in err.lower():
            raise HTTPException(status_code=401, detail="Invalid Groq API key")
        raise HTTPException(status_code=502, detail=f"Groq API error: {err}")

    llm_response = _parse_llm_response(completion.choices[0].message.content)
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
                    if chart_spec and chart_spec.get("type") and chart_spec["type"] != "null":
                        figure = build_figure(rows, chart_spec)
                    sql_error = None
                    break
                except Exception as e:
                    sql_error = str(e)
                    if attempt < MAX_SQL_RETRIES - 1:
                        fixed = _retry_fix_sql(groq_client, req.message, sql, sql_error)
                        if fixed:
                            try:
                                sql = _sanitize_sql(fixed)
                            except ValueError:
                                break  # fixed SQL contained forbidden keyword
                        else:
                            break  # LLM couldn't produce a fix

    # --- Stage 2: Generate text from actual query results ---
    if sql and not sql_error and rows:
        text = _generate_summary(groq_client, req.message, rows)
    elif sql and sql_error:
        text = fallback_text + f"\n\n_(Query failed: {sql_error})_"
    else:
        # Conversational — LLM text is the answer
        text = fallback_text

    return ChatResponse(
        text=text,
        figure=figure,
        sql=sql,
        error=sql_error,
    )


def _generate_summary(
    groq_client: Groq,
    question: str,
    rows: list[dict],
) -> str:
    """Make a second lightweight call to generate text grounded in actual query results."""
    sample = rows[:SUMMARIZE_ROWS]
    results_text = json.dumps(sample, default=str, indent=None)

    prompt = (
        f"The user asked: {question}\n\n"
        f"The database returned these results (first {len(sample)} rows):\n{results_text}\n\n"
        "Write a concise 1-3 sentence answer grounded in these exact results. "
        "Name specific players/teams/numbers from the data. Do not guess or use outside knowledge. "
        "Return plain text only, no JSON."
    )
    try:
        resp = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=256,
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        # Fall back to first row summary if the second call fails
        top = rows[0]
        keys = list(top.keys())
        return f"Top result: {', '.join(f'{k}={top[k]}' for k in keys[:4])}"


def _retry_fix_sql(
    groq_client: Groq,
    question: str,
    failed_sql: str,
    error: str,
) -> str | None:
    """Ask the LLM to fix a failing SQL query. Returns corrected SQL or None."""
    prompt = (
        f"The following DuckDB SQL query failed:\n\n{failed_sql}\n\n"
        f"Error: {error}\n\n"
        f"Original question: {question}\n\n"
        "Return ONLY the corrected SQL — no JSON, no explanation, no code fences."
    )
    try:
        resp = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=512,
        )
        fixed = resp.choices[0].message.content.strip()
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
