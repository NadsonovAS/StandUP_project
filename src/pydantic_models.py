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
    transcribe_json: Optional[List[Dict]] = None
    llm_chapter_json: Optional[Dict[str, Any]] = None
    llm_summarize_json: Optional[Dict[str, Any]] = None
    llm_classifier_json: Optional[List[str]] = None
    sound_classifier_json: Optional[Dict[str, Any]] = None
    audio_path: Optional[str] = None
    process_status: Optional[str] = None


class LlmResponseTheme(BaseModel):
    id: List[int] = Field(
        ...,
        description="array of numbers (IDs of the starting positions for each theme)",
    )
    theme: List[str] = Field(..., description="array of strings (themes in Russian)")


class LlmResponseSummarize(BaseModel):
    summarize: List[str] = Field(
        ..., description="array of strings (summarize in English)"
    )


class LlmResponseClassifier(BaseModel):
    main_category: str = Field(..., description="Exact main category name from YAML.")
    subcategory: str = Field(..., description="Exact subcategory name from YAML.")
    reason: str = Field(
        ..., description="Short explanation of why this category was chosen."
    )
