-- initdb/init_schema.sql
-- 1) Create DWH schemas
CREATE SCHEMA IF NOT EXISTS standup_raw;

-- CREATE SCHEMA IF NOT EXISTS standup_core;

-- CREATE SCHEMA IF NOT EXISTS standup_marts;

-- CREATE SCHEMA IF NOT EXISTS superset;

-- 2) Create Raw Table
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
    laugh_events_json JSONB,
    process_status TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    meta_updated_at TIMESTAMPTZ
);

-- 3) Create funcation
CREATE OR REPLACE FUNCTION update_timestamp_on_meta_change()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.video_meta_json IS DISTINCT FROM OLD.video_meta_json THEN
        NEW.meta_updated_at := now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_timestamp_on_meta_change
BEFORE UPDATE ON standup_raw.process_video
FOR EACH ROW
EXECUTE FUNCTION update_timestamp_on_meta_change();
