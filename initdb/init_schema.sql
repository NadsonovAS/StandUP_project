-- initdb/init_schema.sql

-- 1) Create DWH schemas
CREATE SCHEMA IF NOT EXISTS standup_raw;

CREATE SCHEMA IF NOT EXISTS standup_core;

CREATE SCHEMA IF NOT EXISTS standup_mart;

-- 2) Create Table
-- RAW Tables
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

-- Core Tables
CREATE TABLE standup_core.channels (
  channel_id   TEXT PRIMARY KEY,
  channel_name TEXT NOT NULL,
  created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE standup_core.playlists (
  playlist_id   TEXT PRIMARY KEY,
  playlist_title TEXT,
  created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE standup_core.videos (
  video_id     TEXT PRIMARY KEY,
  playlist_id  TEXT REFERENCES standup_core.playlists(playlist_id),
  channel_id  TEXT REFERENCES standup_core.channels(channel_id),
  video_title  TEXT,
  video_url    TEXT,
  duration   INTEGER,
  like_count   BIGINT,
  view_count   BIGINT,
  comment_count INTEGER,
  upload_date  DATE,
  created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE standup_core.transcript_segments (
  video_id   TEXT REFERENCES standup_core.videos(video_id) ON DELETE CASCADE,
  segment_id INTEGER,
  start_s    NUMERIC,
  end_s      NUMERIC,
  text       TEXT,
  PRIMARY KEY (video_id, segment_id)
);

CREATE TABLE standup_core.llm_chapters (
  chapter_id       SERIAL PRIMARY KEY,
  video_id         TEXT NOT NULL,
  start_segment_id INTEGER NOT NULL,
  end_segment_id   INTEGER NOT NULL,
  theme            TEXT,
  summary          TEXT,
  UNIQUE (video_id, start_segment_id, end_segment_id),
  FOREIGN KEY (video_id, start_segment_id) REFERENCES standup_core.transcript_segments(video_id, segment_id) ON DELETE CASCADE,
  FOREIGN KEY (video_id, end_segment_id)   REFERENCES standup_core.transcript_segments(video_id, segment_id) ON DELETE CASCADE
);

CREATE TABLE standup_core.llm_categories (
  category_id   SERIAL PRIMARY KEY,
  main_category TEXT UNIQUE NOT NULL
);

CREATE TABLE standup_core.llm_subcategories (
  subcategory_id SERIAL PRIMARY KEY,
  category_id    INTEGER NOT NULL REFERENCES standup_core.llm_categories(category_id) ON DELETE CASCADE,
  subcategory    TEXT NOT NULL,
  UNIQUE (category_id, subcategory)
);

CREATE TABLE standup_core.llm_classifications (
  classification_id SERIAL PRIMARY KEY,
  chapter_id       INTEGER NOT NULL REFERENCES standup_core.llm_chapters(chapter_id) ON DELETE CASCADE,
  category_id      INTEGER NOT NULL REFERENCES standup_core.llm_categories(category_id) ON DELETE RESTRICT,
  subcategory_id   INTEGER REFERENCES standup_core.llm_subcategories(subcategory_id) ON DELETE RESTRICT,
  reason           TEXT,
  UNIQUE (chapter_id, category_id, subcategory_id)
);

CREATE TABLE standup_core.sound_features (
  video_id   TEXT REFERENCES standup_core.videos(video_id) ON DELETE CASCADE,
  time_offset NUMERIC,
  score       NUMERIC,
  PRIMARY KEY (video_id, time_offset)
);