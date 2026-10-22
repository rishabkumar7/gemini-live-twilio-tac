from __future__ import annotations

import json

import pytest

from tac_gemini_live import DashboardEventBus


@pytest.mark.asyncio
async def test_dashboard_event_bus_builds_complete_call_state() -> None:
    bus = DashboardEventBus()
    await bus.publish(
        {
            "type": "call.started",
            "conversation_id": "CA123",
            "call_sid": "CA123",
            "stream_sid": "MZ123",
            "model": "gemini-live-test",
            "timestamp": "2026-10-21T20:00:00+00:00",
        }
    )
    await bus.publish(
        {
            "type": "model.connected",
            "conversation_id": "CA123",
            "timestamp": "2026-10-21T20:00:01+00:00",
        }
    )
    await bus.publish(
        {
            "type": "transcript.partial",
            "conversation_id": "CA123",
            "role": "user",
            "text": "I need an oil change",
            "timestamp": "2026-10-21T20:00:02+00:00",
        }
    )
    await bus.publish(
        {
            "type": "transcript.final",
            "conversation_id": "CA123",
            "role": "user",
            "text": "I need an oil change.",
            "timestamp": "2026-10-21T20:00:03+00:00",
        }
    )
    await bus.publish(
        {
            "type": "tool.started",
            "conversation_id": "CA123",
            "tool_id": "tool-1",
            "name": "check_appointment_availability",
            "arguments": {"service": "oil change", "day": "Tuesday"},
            "timestamp": "2026-10-21T20:00:04+00:00",
        }
    )
    await bus.publish(
        {
            "type": "tool.completed",
            "conversation_id": "CA123",
            "tool_id": "tool-1",
            "result": {"available_times": ["9:30 AM"]},
            "duration_ms": 401,
            "timestamp": "2026-10-21T20:00:05+00:00",
        }
    )
    await bus.publish(
        {
            "type": "call.interrupted",
            "conversation_id": "CA123",
            "timestamp": "2026-10-21T20:00:06+00:00",
        }
    )
    await bus.publish(
        {
            "type": "call.ended",
            "conversation_id": "CA123",
            "duration_ms": 6_000,
            "timestamp": "2026-10-21T20:00:06+00:00",
        }
    )

    state = await bus.snapshot()

    assert state["status"] == "ended"
    assert state["call"]["model"] == "gemini-live-test"
    assert state["call"]["duration_ms"] == 6_000
    assert state["transcript"] == [
        {
            "role": "user",
            "text": "I need an oil change.",
            "at": "2026-10-21T20:00:03+00:00",
        }
    ]
    assert state["partial_transcript"] == {}
    assert state["tools"][0]["status"] == "complete"
    assert state["tools"][0]["duration_ms"] == 401
    assert state["interruptions"] == 1


@pytest.mark.asyncio
async def test_dashboard_listener_starts_with_current_snapshot() -> None:
    bus = DashboardEventBus()
    listener = bus.listen()

    message = await anext(listener)
    payload = json.loads(message.split("data: ", 1)[1])
    await listener.aclose()

    assert message.startswith("event: state\n")
    assert payload["status"] == "idle"
