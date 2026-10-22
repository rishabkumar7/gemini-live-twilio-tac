from __future__ import annotations

import base64

import pytest

from tac_gemini_live.audio import TWILIO_FRAME_BYTES, TwilioGeminiAudioBridge


def test_twilio_mulaw_is_resampled_to_gemini_pcm() -> None:
    bridge = TwilioGeminiAudioBridge()
    payload = base64.b64encode(b"\xff" * TWILIO_FRAME_BYTES).decode("ascii")

    pcm = bridge.twilio_payload_to_gemini_pcm(payload)

    assert 630 <= len(pcm) <= 640
    assert len(pcm) % 2 == 0


def test_gemini_pcm_is_chunked_into_twilio_frames() -> None:
    bridge = TwilioGeminiAudioBridge()
    pcm_100_ms = b"\x00\x00" * 2_400

    frames = bridge.gemini_pcm_to_twilio_frames(pcm_100_ms)

    assert len(frames) == 5
    assert all(len(frame) == TWILIO_FRAME_BYTES for frame in frames)
    assert bridge.flush_twilio_output() == b""


def test_output_reset_discards_partial_audio() -> None:
    bridge = TwilioGeminiAudioBridge()
    bridge.gemini_pcm_to_twilio_frames(b"\x00\x00" * 120)

    bridge.reset_output()

    assert bridge.flush_twilio_output() == b""


def test_invalid_twilio_base64_is_rejected() -> None:
    bridge = TwilioGeminiAudioBridge()

    with pytest.raises(ValueError, match="valid base64"):
        bridge.twilio_payload_to_gemini_pcm("not base64!")


def test_odd_length_gemini_pcm_is_rejected() -> None:
    bridge = TwilioGeminiAudioBridge()

    with pytest.raises(ValueError, match="complete 16-bit samples"):
        bridge.gemini_pcm_to_twilio_frames(b"\x00")
