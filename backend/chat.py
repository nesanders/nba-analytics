import json
import os
import re
from typing import Any

from fastapi import APIRouter, Header, HTTPException
from groq import Groq
from pydantic import BaseModel

from chart_builder import build_figure
from db import run_query
from schema_context import SYSTEM_PROMPT

router = APIRouter()

MAX_HISTORY_TURNS = 10  # keep last N user+assistant pairs
MAX_ROWS = 500          # hard cap on rows returned to chart builder


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
async def chat(
    req: ChatRequest,
    x_groq_key: str = Header(..., alias="X-Groq-Key"),
):
    groq = Groq(api_key=x_groq_key)

    # Build message history (trim to last N turns)
    history = req.history[-(MAX_HISTORY_TURNS * 2):]
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in history:
        messages.append({"role": msg.role, "content": msg.content})
    messages.append({"role": "user", "content": req.message})

    # Call Groq
    try:
        completion = groq.chat.completions.create(
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

    raw = completion.choices[0].message.content
    llm_response = _parse_llm_response(raw)

    text = llm_response.get("text", "")
    sql = llm_response.get("sql")
    chart_spec = llm_response.get("chart")

    # Execute SQL
    figure = None
    sql_error = None
    if sql:
        sql = _sanitize_sql(sql)
        try:
            rows = run_query(sql)
            if len(rows) > MAX_ROWS:
                rows = rows[:MAX_ROWS]
            if chart_spec and chart_spec.get("type") and chart_spec["type"] != "null":
                figure = build_figure(rows, chart_spec)
        except Exception as e:
            sql_error = str(e)
            text += f"\n\n_(Query failed: {sql_error})_"

    return ChatResponse(
        text=text,
        figure=figure,
        sql=sql,
        error=sql_error,
    )


def _parse_llm_response(raw: str) -> dict[str, Any]:
    """Parse the LLM JSON response, with fallback for malformed output."""
    # Strip markdown code fences if the model disobeyed instructions
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Best-effort: extract text field at minimum
        text_match = re.search(r'"text"\s*:\s*"((?:[^"\\]|\\.)*)"', raw)
        return {"text": text_match.group(1) if text_match else raw, "sql": None, "chart": None}


def _sanitize_sql(sql: str) -> str:
    """Basic SQL safety check — reject write operations."""
    normalized = sql.strip().upper()
    forbidden = ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "REPLACE")
    for kw in forbidden:
        # Check if keyword appears as a statement start (not inside a string)
        if re.search(rf"\b{kw}\b", normalized):
            raise ValueError(f"SQL contains forbidden keyword: {kw}")
    return sql
