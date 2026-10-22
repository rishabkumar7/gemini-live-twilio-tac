"""Small in-memory event bus used by the demo dashboard."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncGenerator
from copy import deepcopy
from typing import Any


def _empty_state() -> dict[str, Any]:
    return {
        "status": "idle",
        "call": None,
        "transcript": [],
        "partial_transcript": {},
        "tools": [],
        "interruptions": 0,
        "last_event": None,
    }


class DashboardEventBus:
    """Fan out complete dashboard snapshots over Server-Sent Events."""

    def __init__(self) -> None:
        self._state = _empty_state()
        self._event_id = 0
        self._lock = asyncio.Lock()
        self._subscribers: set[asyncio.Queue[str]] = set()

    async def publish(self, event: dict[str, Any]) -> None:
        async with self._lock:
            self._event_id += 1
            stamped = {**event, "id": self._event_id}
            self._reduce(stamped)
            payload = self._serialize_state()
            subscribers = tuple(self._subscribers)

        for queue in subscribers:
            if queue.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
            queue.put_nowait(payload)

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            return deepcopy(self._state)

    async def listen(self) -> AsyncGenerator[str, None]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=16)
        async with self._lock:
            self._subscribers.add(queue)
            initial = self._serialize_state()

        try:
            yield _sse_message(initial)
            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=15)
                    yield _sse_message(payload)
                except TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            async with self._lock:
                self._subscribers.discard(queue)

    def _serialize_state(self) -> str:
        return json.dumps(self._state, separators=(",", ":"), default=str)

    def _reduce(self, event: dict[str, Any]) -> None:
        event_type = event.get("type")
        if event_type == "call.started":
            self._state = {
                **_empty_state(),
                "status": "connecting",
                "call": {
                    "conversation_id": event.get("conversation_id"),
                    "call_sid": event.get("call_sid"),
                    "stream_sid": event.get("stream_sid"),
                    "model": event.get("model"),
                    "started_at": event.get("timestamp"),
                    "ended_at": None,
                    "duration_ms": None,
                },
            }
        elif event_type == "model.connected":
            self._state["status"] = "live"
        elif event_type == "call.interrupted":
            self._state["interruptions"] += 1
        elif event_type == "transcript.partial":
            role = str(event.get("role") or "")
            if role:
                self._state["partial_transcript"][role] = event.get("text", "")
        elif event_type == "transcript.final":
            role = str(event.get("role") or "")
            self._state["transcript"].append(
                {"role": role, "text": event.get("text", ""), "at": event.get("timestamp")}
            )
            self._state["partial_transcript"].pop(role, None)
        elif event_type == "tool.started":
            self._state["tools"].append(
                {
                    "id": event.get("tool_id"),
                    "name": event.get("name"),
                    "arguments": event.get("arguments", {}),
                    "status": "running",
                    "started_at": event.get("timestamp"),
                    "duration_ms": None,
                    "result": None,
                }
            )
            self._state["tools"] = self._state["tools"][-6:]
        elif event_type == "tool.completed":
            for tool in reversed(self._state["tools"]):
                if tool["id"] == event.get("tool_id"):
                    tool.update(
                        {
                            "status": "complete",
                            "duration_ms": event.get("duration_ms"),
                            "result": event.get("result"),
                        }
                    )
                    break
        elif event_type == "call.ended":
            self._state["status"] = "ended"
            if self._state["call"] is not None:
                self._state["call"].update(
                    {
                        "ended_at": event.get("timestamp"),
                        "duration_ms": event.get("duration_ms"),
                    }
                )

        self._state["last_event"] = {
            "id": event.get("id"),
            "type": event_type,
            "timestamp": event.get("timestamp"),
        }


def _sse_message(payload: str) -> str:
    return f"event: state\ndata: {payload}\n\n"
