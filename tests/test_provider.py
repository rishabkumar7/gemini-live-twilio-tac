from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any, cast

import pytest
from tac import TAC
from tac.channels.voice import VoiceChannel
from tac.models.voice import TwiMLRequest
from tac.tools import function_tool

from tac_gemini_live import GeminiLiveProviderConfig
from tac_gemini_live.provider import GeminiLiveProvider, _merge_transcription


def _tac() -> TAC:
    return TAC(
        {
            "account_sid": "ACtest123",
            "auth_token": "test-token",
            "api_key": "SKtest123",
            "api_secret": "test-secret",
            "phone_number": "+15551234567",
            "voice_public_domain": "demo.example.com",
        }
    )


def _start_message() -> dict[str, Any]:
    return {
        "streamSid": "MZ123",
        "callSid": "CA123",
        "mediaFormat": {
            "encoding": "audio/x-mulaw",
            "sampleRate": 8000,
            "channels": 1,
        },
    }


class FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    async def accept(self) -> None:
        pass

    async def receive_json(self) -> Any:
        raise AssertionError("not used")

    async def send_text(self, data: str) -> None:
        self.sent.append(json.loads(data))

    async def close(self) -> None:
        pass


class FakeLiveSession:
    def __init__(self) -> None:
        self.tool_responses: list[Any] = []

    async def send_tool_response(self, *, function_responses: Any) -> None:
        self.tool_responses.append(function_responses)


def _channel(
    *,
    tools: list[Any] | None = None,
    event_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
) -> VoiceChannel:
    return VoiceChannel(
        _tac(),
        config=GeminiLiveProviderConfig(
            api_key="test-gemini-key",
            tools=tools or [],
            event_handler=event_handler,
            live_config={"response_modalities": ["AUDIO"]},
        ),
    )


def test_config_requires_audio_response_modality() -> None:
    with pytest.raises(ValueError, match="response_modalities"):
        GeminiLiveProviderConfig(
            api_key="test-gemini-key",
            live_config={"response_modalities": ["TEXT"]},
        )


@pytest.mark.asyncio
async def test_inbound_twiml_uses_tac_media_stream_route() -> None:
    channel = _channel()

    twiml = await channel.handle_incoming_call(TwiMLRequest.from_form({"CallSid": "CA123"}))

    assert "<Connect>" in twiml
    assert 'url="wss://demo.example.com/ws"' in twiml


@pytest.mark.asyncio
async def test_interruption_clears_twilio_buffer_and_audio_is_streamed() -> None:
    events: list[dict[str, Any]] = []

    async def capture_event(event: dict[str, Any]) -> None:
        events.append(event)

    channel = _channel(event_handler=capture_event)
    provider = cast(GeminiLiveProvider, channel._provider)
    websocket = FakeWebSocket()
    provider._register_call(_start_message(), websocket)

    response = SimpleNamespace(
        tool_call=None,
        server_content=SimpleNamespace(
            interrupted=True,
            input_transcription=None,
            output_transcription=SimpleNamespace(text="Hello there."),
            model_turn=SimpleNamespace(
                parts=[SimpleNamespace(inline_data=SimpleNamespace(data=b"\x00\x00" * 480))]
            ),
            turn_complete=True,
        ),
    )

    await provider._dispatch_gemini_response("CA123", response)

    assert websocket.sent[0] == {"event": "clear", "streamSid": "MZ123"}
    assert websocket.sent[1]["event"] == "media"
    session = channel._conversations["CA123"]
    assert session.metadata["transcript"] == [{"role": "assistant", "text": "Hello there."}]
    assert [event["type"] for event in events] == [
        "call.interrupted",
        "transcript.partial",
        "transcript.final",
    ]


@pytest.mark.asyncio
async def test_tac_tool_result_is_returned_to_gemini() -> None:
    @function_tool()
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    events: list[dict[str, Any]] = []

    async def capture_event(event: dict[str, Any]) -> None:
        events.append(event)

    channel = _channel(tools=[add], event_handler=capture_event)
    provider = cast(GeminiLiveProvider, channel._provider)
    websocket = FakeWebSocket()
    provider._register_call(_start_message(), websocket)
    live_session = FakeLiveSession()
    provider._calls["CA123"].live_session = live_session
    function_call = SimpleNamespace(id="tool-1", name="add", args={"a": 2, "b": 3})

    await provider._handle_tool_call("CA123", function_call)

    assert len(live_session.tool_responses) == 1
    result = live_session.tool_responses[0]
    assert result.id == "tool-1"
    assert result.name == "add"
    assert result.response == {"result": 5}
    assert [event["type"] for event in events] == ["tool.started", "tool.completed"]
    assert events[-1]["result"] == 5


@pytest.mark.parametrize(
    ("current", "update", "expected"),
    [
        ("", "hello", "hello"),
        ("hel", "hello", "hello"),
        ("hello", "lo", "hello"),
        ("hello ", "world", "hello world"),
    ],
)
def test_merge_transcription(current: str, update: str, expected: str) -> None:
    assert _merge_transcription(current, update) == expected
