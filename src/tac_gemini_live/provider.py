"""Twilio Agent Connect provider that bridges Media Streams to Gemini Live."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import time
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from google import genai
from google.genai import types
from tac.channels.voice.media_streams.twiml import TwiMLBuilderMediaStreams
from tac.channels.voice.provider import VoiceProvider
from tac.channels.websocket_protocol import WebSocketDisconnectError, WebSocketProtocol
from tac.core.config import TACConfig
from tac.models.outbound import (
    InitiateVoiceConversationOptions,
    InitiateVoiceConversationResult,
)
from tac.models.session import ConversationSession
from tac.models.stream import StreamStartMessage
from tac.models.voice import TwiMLRequest, VoiceTwiMLOptions, VoiceTwiMLOptionsMediaStreams
from tac.tools import TACTool

from .audio import TwilioGeminiAudioBridge
from .config import GeminiLiveProviderConfig

GEMINI_INPUT_MIME_TYPE = "audio/pcm;rate=16000"


@dataclass
class _CallState:
    twilio_ws: WebSocketProtocol
    stream_sid: str
    audio: TwilioGeminiAudioBridge = field(default_factory=TwilioGeminiAudioBridge)
    live_context: Any = None
    live_session: Any = None
    reader_task: asyncio.Task[None] | None = None
    tool_tasks: set[asyncio.Task[None]] = field(default_factory=set)
    user_transcript: str = ""
    assistant_transcript: str = ""
    started_at: float = field(default_factory=time.monotonic)


class GeminiLiveProvider(VoiceProvider):
    """A full-duplex Gemini Live implementation of TAC's ``VoiceProvider``."""

    config: GeminiLiveProviderConfig

    def __init__(
        self,
        channel: Any,
        tac_config: TACConfig,
        config: GeminiLiveProviderConfig,
    ) -> None:
        super().__init__(channel)
        self.config = config
        self.tac_config = tac_config
        self._twiml = TwiMLBuilderMediaStreams(tac_config, config)
        self._client = genai.Client(api_key=config.api_key)
        self._calls: dict[str, _CallState] = {}
        self._tools_by_name: dict[str, TACTool] = {tool.name: tool for tool in config.tools}
        self._call_live_configs: dict[str, dict[str, Any]] = {}

    @property
    def channel_name(self) -> str:
        return "VOICE_MEDIA_STREAM_GEMINI_LIVE"

    def get_websocket(self, conversation_id: str) -> WebSocketProtocol | None:
        state = self._calls.get(conversation_id)
        return state.twilio_ws if state else None

    def get_transcript(self, conversation_id: str) -> list[dict[str, str]]:
        session = self.channel._conversations.get(conversation_id)
        if session is None:
            return []
        return list(session.metadata.get("transcript", []))

    async def handle_incoming_call(
        self,
        twiml_request: TwiMLRequest | None = None,
        *,
        host_twiml_options: VoiceTwiMLOptions | None = None,
    ) -> str:
        if host_twiml_options is not None and not isinstance(
            host_twiml_options, VoiceTwiMLOptionsMediaStreams
        ):
            raise TypeError("GeminiLiveProvider requires VoiceTwiMLOptionsMediaStreams")

        customized: VoiceTwiMLOptionsMediaStreams | None = None
        if self.channel._on_inbound_call_twiml is not None and twiml_request is not None:
            candidate = await self.channel._on_inbound_call_twiml(twiml_request)
            if not isinstance(candidate, VoiceTwiMLOptionsMediaStreams):
                raise TypeError(
                    "GeminiLiveProvider's TwiML customizer must return "
                    "VoiceTwiMLOptionsMediaStreams"
                )
            customized = candidate

        if (
            self.config.on_inbound_call_live_config is not None
            and twiml_request is not None
            and twiml_request.call_sid is not None
        ):
            live_config = await self.config.on_inbound_call_live_config(twiml_request)
            if live_config is not None:
                self._call_live_configs[twiml_request.call_sid] = live_config

        return self._twiml.build(
            "handle_incoming_call", host=host_twiml_options, per_call=customized
        )

    async def initiate_outbound_conversation(
        self,
        options: InitiateVoiceConversationOptions,
    ) -> InitiateVoiceConversationResult:
        if options.twiml_options is not None and not isinstance(
            options.twiml_options, VoiceTwiMLOptionsMediaStreams
        ):
            raise TypeError("Gemini Live outbound calls require VoiceTwiMLOptionsMediaStreams")

        twiml = self._twiml.build(
            "initiate_outbound_conversation",
            per_call=options.twiml_options,
            websocket_url=options.websocket_url,
        )
        call_kwargs = options.call_options.to_call_kwargs() if options.call_options else {}
        call_kwargs = self._apply_call_event_callbacks(call_kwargs)
        client = self.channel._get_twilio_client()
        call = await asyncio.to_thread(
            client.calls.create,
            to=options.to,
            from_=self.channel.tac.config.phone_number,
            twiml=twiml,
            **call_kwargs,
        )
        return InitiateVoiceConversationResult(call_sid=call.sid)

    async def handle_websocket(self, websocket: WebSocketProtocol) -> None:
        await websocket.accept()
        conversation_id: str | None = None

        try:
            while True:
                receive_task = asyncio.create_task(websocket.receive_json())
                waiters: list[asyncio.Task[Any]] = [receive_task]
                state = self._calls.get(conversation_id) if conversation_id else None
                if state is not None and state.reader_task is not None:
                    waiters.append(state.reader_task)

                done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
                if state is not None and state.reader_task in done:
                    receive_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await receive_task
                    await state.reader_task
                    break

                message = receive_task.result()
                event = message.get("event")
                if event == "start":
                    conversation_id = self._register_call(message.get("start") or {}, websocket)
                    state = self._calls[conversation_id]
                    await self._emit(
                        "call.started",
                        conversation_id,
                        call_sid=conversation_id,
                        stream_sid=state.stream_sid,
                        model=self.config.model,
                    )
                    await self._connect_gemini(conversation_id)
                    state.reader_task = asyncio.create_task(self._read_gemini(conversation_id))
                elif event == "media" and conversation_id is not None:
                    payload = (message.get("media") or {}).get("payload")
                    if payload:
                        await self._send_twilio_audio_to_gemini(conversation_id, payload)
                elif event == "stop":
                    break
        except WebSocketDisconnectError:
            self.logger.info("Twilio Media Stream disconnected", conversation_id=conversation_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.logger.error(
                "Gemini Live Media Stream failed",
                conversation_id=conversation_id,
                error=str(exc),
                exc_info=True,
            )
        finally:
            if conversation_id is not None:
                await self._cleanup_call(conversation_id)

    def _register_call(self, start: dict[str, Any], websocket: WebSocketProtocol) -> str:
        message = StreamStartMessage(**start)
        media_format = message.media_format or {}
        if (
            media_format.get("encoding") != "audio/x-mulaw"
            or int(media_format.get("sampleRate", 0)) != 8_000
        ):
            raise ValueError(f"Unexpected Twilio Media Streams format: {media_format!r}")

        conversation_id = message.conversation_id
        self._calls[conversation_id] = _CallState(
            twilio_ws=websocket,
            stream_sid=message.stream_sid,
        )
        session = self.channel._start_conversation(conversation_id, profile_id=None)
        session.call_sid = message.call_sid
        session.metadata.update(
            {
                "stream_sid": message.stream_sid,
                "gemini_model": self.config.model,
                "transcript": [],
            }
        )
        return conversation_id

    async def _connect_gemini(self, conversation_id: str) -> None:
        state = self._calls[conversation_id]
        raw_config = self._call_live_configs.pop(conversation_id, self.config.live_config)
        live_config = deepcopy(raw_config)
        if self.config.tools:
            declarations = [
                types.FunctionDeclaration(
                    name=tool.name,
                    description=tool.description,
                    parameters_json_schema=tool.params_json_schema,
                )
                for tool in self.config.tools
            ]
            configured_tools = list(live_config.get("tools", []))
            configured_tools.append(types.Tool(function_declarations=declarations))
            live_config["tools"] = configured_tools

        state.live_context = self._client.aio.live.connect(
            model=self.config.model,
            config=types.LiveConnectConfig(**live_config),
        )
        state.live_session = await state.live_context.__aenter__()
        self.logger.info(
            "Connected call to Gemini Live",
            conversation_id=conversation_id,
            model=self.config.model,
        )
        await self._emit("model.connected", conversation_id, model=self.config.model)

        if self.config.welcome_instruction:
            await state.live_session.send_client_content(
                turns=types.Content(
                    role="user",
                    parts=[types.Part(text=self.config.welcome_instruction)],
                ),
                turn_complete=True,
            )

    async def _send_twilio_audio_to_gemini(self, conversation_id: str, payload: str) -> None:
        state = self._calls.get(conversation_id)
        if state is None or state.live_session is None:
            return
        pcm = state.audio.twilio_payload_to_gemini_pcm(payload)
        await state.live_session.send_realtime_input(
            audio=types.Blob(data=pcm, mime_type=GEMINI_INPUT_MIME_TYPE)
        )

    async def _read_gemini(self, conversation_id: str) -> None:
        state = self._calls[conversation_id]
        try:
            while True:
                async for response in state.live_session.receive():
                    await self._dispatch_gemini_response(conversation_id, response)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.logger.info(
                "Gemini Live receive loop ended",
                conversation_id=conversation_id,
                error=str(exc),
            )

    async def _dispatch_gemini_response(self, conversation_id: str, response: Any) -> None:
        state = self._calls.get(conversation_id)
        session = self.channel._conversations.get(conversation_id)
        if state is None or session is None:
            return

        tool_call = getattr(response, "tool_call", None)
        if tool_call is not None:
            for function_call in tool_call.function_calls or []:
                task = asyncio.create_task(self._handle_tool_call(conversation_id, function_call))
                state.tool_tasks.add(task)
                task.add_done_callback(state.tool_tasks.discard)

        content = getattr(response, "server_content", None)
        if content is None:
            return

        if content.interrupted:
            state.audio.reset_output()
            await self._emit("call.interrupted", conversation_id)
            await self._send_twilio_event(
                conversation_id,
                {"event": "clear", "streamSid": state.stream_sid},
            )

        input_transcription = getattr(content, "input_transcription", None)
        if input_transcription and input_transcription.text:
            state.user_transcript = _merge_transcription(
                state.user_transcript, input_transcription.text
            )
            await self._emit(
                "transcript.partial",
                conversation_id,
                role="user",
                text=state.user_transcript,
            )
        output_transcription = getattr(content, "output_transcription", None)
        if output_transcription and output_transcription.text:
            state.assistant_transcript = _merge_transcription(
                state.assistant_transcript, output_transcription.text
            )
            await self._emit(
                "transcript.partial",
                conversation_id,
                role="assistant",
                text=state.assistant_transcript,
            )

        model_turn = getattr(content, "model_turn", None)
        if model_turn is not None:
            for part in model_turn.parts or []:
                inline_data = getattr(part, "inline_data", None)
                if inline_data is not None and inline_data.data:
                    await self._send_gemini_audio_to_twilio(
                        conversation_id, bytes(inline_data.data)
                    )

        if content.turn_complete:
            remainder = state.audio.flush_twilio_output()
            if remainder:
                await self._send_twilio_audio(conversation_id, remainder)
            for turn in self._flush_transcript(state, session):
                await self._emit("transcript.final", conversation_id, **turn)

    async def _send_gemini_audio_to_twilio(self, conversation_id: str, pcm_24k: bytes) -> None:
        state = self._calls.get(conversation_id)
        if state is None:
            return
        for frame in state.audio.gemini_pcm_to_twilio_frames(pcm_24k):
            await self._send_twilio_audio(conversation_id, frame)

    async def _send_twilio_audio(self, conversation_id: str, mulaw: bytes) -> None:
        state = self._calls.get(conversation_id)
        if state is None:
            return
        await self._send_twilio_event(
            conversation_id,
            {
                "event": "media",
                "streamSid": state.stream_sid,
                "media": {"payload": base64.b64encode(mulaw).decode("ascii")},
            },
        )

    async def _send_twilio_event(self, conversation_id: str, event: dict[str, Any]) -> None:
        state = self._calls.get(conversation_id)
        if state is None:
            return
        try:
            await state.twilio_ws.send_text(json.dumps(event))
        except (WebSocketDisconnectError, RuntimeError):
            self.logger.debug("Twilio socket closed while sending", conversation_id=conversation_id)

    async def _handle_tool_call(self, conversation_id: str, function_call: Any) -> None:
        state = self._calls.get(conversation_id)
        if state is None or state.live_session is None:
            return

        name = function_call.name or ""
        tool_id = function_call.id or f"{name}-{time.monotonic_ns()}"
        arguments = dict(function_call.args or {})
        started_at = time.monotonic()
        await self._emit(
            "tool.started",
            conversation_id,
            tool_id=tool_id,
            name=name,
            arguments=arguments,
        )
        tool = self._tools_by_name.get(name)
        if tool is None:
            output: object = {"error": f"Unknown tool: {name}"}
        else:
            try:
                output = await tool(**arguments)
            except Exception as exc:
                self.logger.error(
                    "Gemini tool call failed",
                    conversation_id=conversation_id,
                    tool=name,
                    error=str(exc),
                    exc_info=True,
                )
                output = {"error": f"{name} failed"}

        safe_output = json.loads(json.dumps(output, default=str))
        await self._emit(
            "tool.completed",
            conversation_id,
            tool_id=tool_id,
            name=name,
            result=safe_output,
            duration_ms=round((time.monotonic() - started_at) * 1_000),
        )
        await state.live_session.send_tool_response(
            function_responses=types.FunctionResponse(
                id=function_call.id,
                name=name,
                response={"result": safe_output},
            )
        )

    @staticmethod
    def _flush_transcript(state: _CallState, session: ConversationSession) -> list[dict[str, str]]:
        transcript = session.metadata.setdefault("transcript", [])
        completed: list[dict[str, str]] = []
        if state.user_transcript.strip():
            completed.append({"role": "user", "text": state.user_transcript.strip()})
        if state.assistant_transcript.strip():
            completed.append({"role": "assistant", "text": state.assistant_transcript.strip()})
        transcript.extend(completed)
        state.user_transcript = ""
        state.assistant_transcript = ""
        return completed

    async def _cleanup_call(self, conversation_id: str) -> None:
        state = self._calls.pop(conversation_id, None)
        if state is None:
            return

        if state.reader_task is not None and state.reader_task is not asyncio.current_task():
            state.reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await state.reader_task

        for task in state.tool_tasks:
            task.cancel()
        if state.tool_tasks:
            await asyncio.gather(*state.tool_tasks, return_exceptions=True)

        if state.live_session is not None:
            with contextlib.suppress(Exception):
                await state.live_session.send_realtime_input(audio_stream_end=True)
        if state.live_context is not None:
            with contextlib.suppress(Exception):
                await state.live_context.__aexit__(None, None, None)

        session = self.channel._conversations.get(conversation_id)
        if session is not None:
            for turn in self._flush_transcript(state, session):
                await self._emit("transcript.final", conversation_id, **turn)
        await self._emit(
            "call.ended",
            conversation_id,
            duration_ms=round((time.monotonic() - state.started_at) * 1_000),
        )
        await self.channel._end_conversation(conversation_id)

    async def _emit(self, event_type: str, conversation_id: str, **payload: Any) -> None:
        handler = self.config.event_handler
        if handler is None:
            return
        event = {
            "type": event_type,
            "conversation_id": conversation_id,
            "timestamp": datetime.now(UTC).isoformat(),
            **payload,
        }
        try:
            await handler(event)
        except Exception as exc:
            self.logger.warning(
                "Voice dashboard event handler failed",
                event_type=event_type,
                conversation_id=conversation_id,
                error=str(exc),
            )


def _merge_transcription(current: str, update: str) -> str:
    """Handle APIs that emit either deltas or cumulative transcription updates."""
    if not current:
        return update
    if update.startswith(current):
        return update
    if current.endswith(update):
        return current
    return current + update
