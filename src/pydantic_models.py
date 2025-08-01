from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ProcessVideo(BaseModel):
    channel_id: Optional[str] = None
    channel_name: Optional[str] = None
    playlist_id: Optional[str] = None
    playlist_title: Optional[str] = None
    video_id: Optional[str] = None
    video_title: Optional[str] = None
    video_url: Optional[str] = None
    video_meta_json: Optional[Dict[str, Any]] = None
    transcribe_json: Optional[Dict[str, Any]] = None
    llm_chapter_json: Optional[Dict[str, Any]] = None
    sound_classifier: Optional[Dict[str, Any]] = None
    audio_path: Optional[str] = None


class LLMresponse(BaseModel):
    theme: List[str] = Field(..., description="Список тем")
    timestamp: List[int] = Field(..., description="Список меток времени (в секундах)")

    class Config:
        openai_strict = True
