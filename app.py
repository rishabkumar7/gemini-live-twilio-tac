"""Appointment-concierge demo using Twilio Agent Connect and Gemini Live."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from tac import TAC, TACConfig
from tac.channels.voice import VoiceChannel
from tac.models.outbound import InitiateVoiceConversationOptions
from tac.models.session import ConversationSession
from tac.server import TACFastAPIServer
from tac.tools import function_tool

from tac_gemini_live import DashboardEventBus, GeminiLiveProviderConfig

load_dotenv()

STATIC_DIR = Path(__file__).parent / "static"
dashboard_events = DashboardEventBus()


@function_tool()
async def check_appointment_availability(service: str, day: str) -> dict[str, object]:
    """Check demo appointment times for a service on a requested day."""
    await asyncio.sleep(0.4)  # Makes the tool boundary visible in local demos.
    return {
        "service": service,
        "day": day,
        "available_times": ["9:30 AM", "1:00 PM", "3:30 PM"],
        "timezone": "America/Chicago",
    }


@function_tool()
async def book_demo_appointment(name: str, service: str, day: str, time: str) -> dict[str, str]:
    """Book a fictional appointment after the caller explicitly confirms the details."""
    await asyncio.sleep(0.4)
    return {
        "status": "confirmed",
        "confirmation": "DEMO-4821",
        "name": name,
        "service": service,
        "day": day,
        "time": time,
        "note": "This is a demo booking; no real appointment was created.",
    }


tac = TAC(config=TACConfig.from_env())

SYSTEM_INSTRUCTION = """
You are the voice concierge for Northstar Service Center, a fictional demo business.
Speak naturally and keep each turn to one or two short sentences. Never use markdown,
emoji, or bullet lists because every response is played over a phone call. Use the
availability tool before offering times. Before booking, repeat the name, service, day,
and time and obtain an explicit yes. Clearly say that any booking is only a demo.
""".strip()

voice_channel = VoiceChannel(
    tac,
    config=GeminiLiveProviderConfig(
        tools=[check_appointment_availability, book_demo_appointment],
        event_handler=dashboard_events.publish,
        welcome_instruction=(
            "Greet the caller now with: Hello, you've reached the Northstar Service Center "
            "demo assistant, powered by Twilio Agent Connect and Gemini Live. How can I help?"
        ),
        live_config={
            "response_modalities": ["AUDIO"],
            "system_instruction": SYSTEM_INSTRUCTION,
            "speech_config": {
                "language_code": "en-US",
                "voice_config": {
                    "prebuilt_voice_config": {"voice_name": os.getenv("GEMINI_VOICE_NAME", "Kore")}
                },
            },
            "input_audio_transcription": {},
            "output_audio_transcription": {},
            "context_window_compression": {"sliding_window": {}},
        },
    ),
)


@tac.on_conversation_ended
async def print_transcript(session: ConversationSession) -> None:
    print(f"Call {session.call_sid or session.conversation_id} ended. Transcript:")
    for turn in session.metadata.get("transcript", []):
        print(f"  {turn['role']}: {turn['text']}")


async def place_outbound_call(to: str) -> None:
    result = await voice_channel.initiate_outbound_conversation(
        InitiateVoiceConversationOptions(to=to)
    )
    print(f"Outbound call placed to {to}: {result.call_sid}")


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    outbound_to = os.getenv("DEMO_OUTBOUND_TO", "").strip()
    if outbound_to:
        asyncio.create_task(place_outbound_call(outbound_to))
    yield


fastapi_app = FastAPI(title="Twilio Agent Connect + Gemini Live", lifespan=lifespan)
server = TACFastAPIServer(tac=tac, voice_channel=voice_channel, app=fastapi_app)
app = server.app
app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="dashboard-assets")


@app.get("/", include_in_schema=False)
async def dashboard() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/state", include_in_schema=False)
async def dashboard_state() -> dict[str, object]:
    return await dashboard_events.snapshot()


@app.get("/api/events", include_in_schema=False)
async def dashboard_event_stream() -> StreamingResponse:
    return StreamingResponse(
        dashboard_events.listen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "provider": "gemini-live"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the TAC + Gemini Live voice demo")
    parser.add_argument("--to", help="Place one outbound call on startup, in E.164 format")
    args = parser.parse_args()
    if args.to:
        os.environ["DEMO_OUTBOUND_TO"] = args.to
    server.start()
