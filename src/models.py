from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class ProcessVideo(BaseModel):
    channel_id: str | None = None
    channel_name: str | None = None
    playlist_id: str | None = None
    playlist_title: str | None = None
    video_id: str | None = None
    video_title: str | None = None
    video_url: str | None = None

    video_meta_json: dict[str, Any] | None = Field(default_factory=dict)
    transcribe_json: dict[str, Any] | None = Field(default_factory=dict)
    llm_chapter_json: dict[str, Any] | None = Field(default_factory=dict)
    llm_classifier_json: dict[str, Any] | None = Field(default_factory=dict)
    sound_classifier_json: dict[str, Any] | None = Field(default_factory=dict)

    audio_path: str | None = None
    process_status: str | None = None
    meta_updated_at: datetime | None = None
