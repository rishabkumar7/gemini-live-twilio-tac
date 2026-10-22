"""Stateful audio conversion between Twilio Media Streams and Gemini Live."""

from __future__ import annotations

import audioop
import base64
import binascii
from dataclasses import dataclass, field
from typing import Any

TWILIO_SAMPLE_RATE = 8_000
GEMINI_INPUT_SAMPLE_RATE = 16_000
GEMINI_OUTPUT_SAMPLE_RATE = 24_000
SAMPLE_WIDTH_BYTES = 2
TWILIO_FRAME_BYTES = 160  # 20 ms of 8 kHz mu-law audio.


@dataclass
class TwilioGeminiAudioBridge:
    """Convert both call legs while preserving resampler state across chunks."""

    _input_rate_state: Any = None
    _output_rate_state: Any = None
    _twilio_output_buffer: bytearray = field(default_factory=bytearray)

    def twilio_payload_to_gemini_pcm(self, payload: str) -> bytes:
        """Decode base64 mu-law/8k and return signed PCM16/16k little-endian."""
        try:
            mulaw = base64.b64decode(payload, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("Twilio media payload is not valid base64") from exc

        pcm_8k = audioop.ulaw2lin(mulaw, SAMPLE_WIDTH_BYTES)
        pcm_16k, self._input_rate_state = audioop.ratecv(
            pcm_8k,
            SAMPLE_WIDTH_BYTES,
            1,
            TWILIO_SAMPLE_RATE,
            GEMINI_INPUT_SAMPLE_RATE,
            self._input_rate_state,
        )
        return pcm_16k

    def gemini_pcm_to_twilio_frames(self, pcm_24k: bytes) -> list[bytes]:
        """Convert PCM16/24k audio into complete 20 ms mu-law/8k frames."""
        if len(pcm_24k) % SAMPLE_WIDTH_BYTES:
            raise ValueError("Gemini PCM audio must contain complete 16-bit samples")

        pcm_8k, self._output_rate_state = audioop.ratecv(
            pcm_24k,
            SAMPLE_WIDTH_BYTES,
            1,
            GEMINI_OUTPUT_SAMPLE_RATE,
            TWILIO_SAMPLE_RATE,
            self._output_rate_state,
        )
        self._twilio_output_buffer.extend(audioop.lin2ulaw(pcm_8k, SAMPLE_WIDTH_BYTES))

        frames: list[bytes] = []
        while len(self._twilio_output_buffer) >= TWILIO_FRAME_BYTES:
            frames.append(bytes(self._twilio_output_buffer[:TWILIO_FRAME_BYTES]))
            del self._twilio_output_buffer[:TWILIO_FRAME_BYTES]
        return frames

    def flush_twilio_output(self) -> bytes:
        """Return the final partial frame at a model turn boundary, if any."""
        remainder = bytes(self._twilio_output_buffer)
        self._twilio_output_buffer.clear()
        return remainder

    def reset_output(self) -> None:
        """Discard queued/resampler state when Gemini reports an interruption."""
        self._output_rate_state = None
        self._twilio_output_buffer.clear()
