"""
LLM provider abstraction.

Exposes a uniform interface used by chat.py regardless of which backend
is selected:

  client = get_llm_client(groq_key=..., gemini_token=...)
  response_text = client.complete(messages, json_mode=True, max_tokens=1024)

Supported providers:
  - Groq  (llama-3.3-70b-versatile) — user supplies their own API key
  - Gemini Flash (gemini-2.0-flash) — server-held key, gated by GEMINI_AUTH_TOKEN

The GEMINI_API_KEY and GEMINI_AUTH_TOKEN env vars are set on Cloud Run
and never exposed to clients.
"""
import json
import os
import re
from typing import Any

GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY", "")
GEMINI_AUTH_TOKEN = os.getenv("GEMINI_AUTH_TOKEN", "")

GROQ_MODEL   = "llama-3.3-70b-versatile"
GEMINI_MODEL = "gemini-2.5-flash"


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------

def get_llm_client(
    groq_key: str | None = None,
    gemini_token: str | None = None,
) -> "_LLMClient":
    """
    Return the appropriate LLM client.

    - If gemini_token is provided and matches GEMINI_AUTH_TOKEN, return a
      GeminiClient backed by the server-held GEMINI_API_KEY.
    - Otherwise validate groq_key and return a GroqClient.

    Raises HTTPException on auth failure (imported lazily to avoid circular).
    """
    from fastapi import HTTPException

    if gemini_token:
        if not GEMINI_AUTH_TOKEN:
            raise HTTPException(status_code=503, detail="Gemini not configured on this server")
        if gemini_token != GEMINI_AUTH_TOKEN:
            raise HTTPException(status_code=401, detail="Invalid Gemini auth token")
        if not GEMINI_API_KEY:
            raise HTTPException(status_code=503, detail="Gemini API key not configured")
        return GeminiClient()

    if groq_key:
        return GroqClient(groq_key)

    from fastapi import HTTPException
    raise HTTPException(status_code=401, detail="No API key provided")


# ---------------------------------------------------------------------------
# Base interface
# ---------------------------------------------------------------------------

class _LLMClient:
    def complete(
        self,
        messages: list[dict[str, str]],
        *,
        json_mode: bool = False,
        max_tokens: int = 1024,
    ) -> str:
        raise NotImplementedError

    @property
    def provider(self) -> str:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Groq
# ---------------------------------------------------------------------------

class GroqClient(_LLMClient):
    def __init__(self, api_key: str):
        from groq import Groq
        self._client = Groq(api_key=api_key)

    @property
    def provider(self) -> str:
        return "groq"

    def complete(self, messages, *, json_mode=False, max_tokens=1024) -> str:
        from fastapi import HTTPException
        kwargs: dict[str, Any] = dict(
            model=GROQ_MODEL,
            messages=messages,
            temperature=0.1,
            max_tokens=max_tokens,
        )
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}
        try:
            resp = self._client.chat.completions.create(**kwargs)
            return resp.choices[0].message.content
        except Exception as e:
            err = str(e)
            if "401" in err or "invalid_api_key" in err.lower():
                raise HTTPException(status_code=401, detail="Invalid Groq API key")
            raise HTTPException(status_code=502, detail=f"Groq error: {err}")


# ---------------------------------------------------------------------------
# Gemini (google-genai SDK — replaces deprecated google-generativeai)
# ---------------------------------------------------------------------------

class GeminiClient(_LLMClient):
    def __init__(self):
        from google import genai
        from google.genai import types as genai_types
        self._client = genai.Client(api_key=GEMINI_API_KEY)
        self._types = genai_types

    @property
    def provider(self) -> str:
        return "gemini"

    def complete(self, messages, *, json_mode=False, max_tokens=1024) -> str:
        from fastapi import HTTPException

        # Separate system instruction from conversation messages
        system_parts = [m["content"] for m in messages if m["role"] == "system"]
        system_instruction = system_parts[0] if system_parts else None
        conv_msgs = [m for m in messages if m["role"] != "system"]

        # Build google-genai Content objects (role "assistant" → "model")
        contents = []
        for m in conv_msgs:
            role = "model" if m["role"] == "assistant" else "user"
            contents.append(self._types.Content(
                role=role,
                parts=[self._types.Part(text=m["content"])],
            ))

        config_kwargs: dict[str, Any] = {
            "max_output_tokens": max_tokens,
            "temperature": 0.1,
        }
        if json_mode:
            config_kwargs["response_mime_type"] = "application/json"
        if system_instruction:
            config_kwargs["system_instruction"] = system_instruction

        try:
            resp = self._client.models.generate_content(
                model=GEMINI_MODEL,
                contents=contents,
                config=self._types.GenerateContentConfig(**config_kwargs),
            )
            return resp.text
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Gemini error: {e}")
