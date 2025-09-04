-- initdb/init_schema.sql

CREATE SCHEMA IF NOT EXISTS standup_raw;

CREATE TABLE IF NOT EXISTS standup_raw.process_video (
    channel_id TEXT,
    channel_name TEXT,
    playlist_id TEXT,
    playlist_title TEXT,
    video_id TEXT PRIMARY KEY,
    video_title TEXT,
    video_url TEXT,
    video_meta_json JSONB,
    transcribe_json JSONB,
    llm_chapter_json JSONB,
    llm_classifier_json JSONB,
    sound_classifier_json JSONB,
    process_status TEXT
);
