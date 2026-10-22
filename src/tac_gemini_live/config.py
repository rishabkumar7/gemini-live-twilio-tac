"""Configuration model for the Gemini Live TAC voice provider."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from pydantic import Field, model_validator
from tac.channels.voice.media_streams.config import MediaStreamsProviderConfig
from tac.channels.voice.provider import VoiceProvider
from tac.core.config import TACConfig
from tac.models.voice import TwiMLRequest
from tac.tools import TACTool

if TYPE_CHECKING:
    from tac.channels.voice import VoiceChannel


def _default_api_key() -> str | None:
    return os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")


def _default_live_config() -> dict[str, Any]:
    voice = os.getenv("GEMINI_VOICE_NAME", "Kore")
    return {
        "response_modalities": ["AUDIO"],
        "speech_config": {
            "language_code": "en-US",
            "voice_config": {"prebuilt_voice_config": {"voice_name": voice}},
        },
        "input_audio_transcription": {},
        "output_audio_transcription": {},
        "context_window_compression": {"sliding_window": {}},
    }


class GeminiLiveProviderConfig(MediaStreamsProviderConfig):
    """Settings for a Gemini Live session carried by Twilio Media Streams."""

    model_config = {"extra": "forbid", "arbitrary_types_allowed": True}

    api_key: str | None = Field(default_factory=_default_api_key, repr=False)
    model: str = Field(default_factory=lambda: os.getenv("GEMINI_LIVE_MODEL", "gemini-3.8-live"))
    live_config: dict[str, Any] = Field(default_factory=_default_live_config)
    tools: list[TACTool] = Field(default_factory=list)
    welcome_instruction: str | None = None
    on_inbound_call_live_config: (
        Callable[[TwiMLRequest], Awaitable[dict[str, Any] | None]] | None
    ) = None
    event_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = Field(
        default=None,
        exclude=True,
        description="Optional sink for call, transcript, interruption, and tool events.",
    )

    @model_validator(mode="after")
    def _validate_settings(self) -> GeminiLiveProviderConfig:
        if not self.api_key:
            raise ValueError(
                "api_key is required; set GEMINI_API_KEY (or GOOGLE_API_KEY), "
                "or pass api_key explicitly"
            )
        modalities = self.live_config.get("response_modalities")
        if modalities != ["AUDIO"]:
            raise ValueError("live_config.response_modalities must be exactly ['AUDIO']")
        return self

    def create_provider(self, channel: VoiceChannel, tac_config: TACConfig) -> VoiceProvider:
        from .provider import GeminiLiveProvider

        return GeminiLiveProvider(channel, tac_config, self)
