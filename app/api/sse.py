"""SSE framing helpers."""

from __future__ import annotations

import json
from collections.abc import Mapping


SSE_HEADERS: dict[str, str] = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}


def format_event(event: str, data: Mapping[str, object] | None = None) -> str:
    payload = json.dumps(data, ensure_ascii=False, default=str) if data is not None else "{}"
    return f"event: {event}\ndata: {payload}\n\n"


def format_comment(text: str) -> str:
    return f": {text}\n\n"
