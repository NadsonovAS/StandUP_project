-- initdb/init_schema.sql

CREATE SCHEMA IF NOT EXISTS standup_raw;

CREATE TABLE IF NOT EXISTS standup_raw.process_video (
channel_id TEXT NOT NULL,
    channel_name TEXT,
    playlist_id TEXT,
    playlist_title TEXT,
    video_id TEXT PRIMARY KEY,
    video_title TEXT,
    viedeo_url TEXT,
    duration INTEGER,
    view_count INTEGER,
    comment_count INTEGER,
    like_count INTEGER,
    upload_date TEXT,
    audio_path TEXT,
    transcribe_json JSONB,
    llm_chapter_json JSONB,
    sound_classifier JSONB
);
