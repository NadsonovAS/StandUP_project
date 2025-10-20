-- initdb/init_schema.sql
-- 1) Create DWH schemas
CREATE SCHEMA IF NOT EXISTS standup_raw;

CREATE SCHEMA IF NOT EXISTS standup_core;

CREATE SCHEMA IF NOT EXISTS standup_dds;

CREATE SCHEMA IF NOT EXISTS superset;

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

-- 2) Create Core Table
CREATE TABLE IF NOT EXISTS standup_core.core_channels (
    channel_id TEXT PRIMARY KEY,
    channel_name TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS standup_core.core_playlists (
    playlist_id TEXT PRIMARY KEY,
    playlist_title TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS standup_core.core_videos (
    video_id TEXT PRIMARY KEY,
    playlist_id TEXT NOT NULL REFERENCES standup_core.core_playlists (
        playlist_id
    ),
    channel_id TEXT NOT NULL REFERENCES standup_core.core_channels (channel_id),
    video_title TEXT NOT NULL,
    duration INT2 NOT NULL,
    upload_date DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS standup_core.core_videos_meta (
    video_id TEXT REFERENCES standup_core.core_videos (
        video_id
    ) ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,
    like_count INT4 NOT NULL,
    view_count INT4 NOT NULL,
    comment_count INT4 NOT NULL,
    PRIMARY KEY (video_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS standup_core.core_transcript_segments (
    video_id TEXT REFERENCES standup_core.core_videos (
        video_id
    ) ON DELETE CASCADE,
    segment_id INT2 NOT NULL,
    start_s FLOAT4 NOT NULL,
    end_s FLOAT4 NOT NULL,
    segment_text TEXT NOT NULL,
    PRIMARY KEY (video_id, segment_id)
);


CREATE TABLE IF NOT EXISTS standup_core.core_categories (
    category_id INT2 PRIMARY KEY,
    main_category TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS standup_core.core_subcategories (
    subcategory_id INT2 PRIMARY KEY,
    category_id INT2 NOT NULL REFERENCES standup_core.core_categories (
        category_id
    ) ON DELETE CASCADE,
    subcategory TEXT NOT NULL,
    UNIQUE (category_id, subcategory)
);

-- Seed lookup data for LLM categories and subcategories
INSERT INTO
standup_core.core_categories (category_id, main_category)
VALUES
(1, 'Advertising'),
(2, 'Politics & Society'),
(3, 'Economy, Work & Money'),
(4, 'Health, Body & Mind'),
(5, 'Relationships & Social Life'),
(6, 'Science, Technology & Digital Life'),
(7, 'Culture, Arts & Media'),
(8, 'Environment & Planet'),
(9, 'History, Identity & Heritage'),
(10, 'Dark, Edgy & Absurd Humor') ON CONFLICT (category_id) DO NOTHING;

INSERT INTO
standup_core.core_subcategories (subcategory_id, category_id, subcategory)
VALUES
(1, 1, 'Upcoming shows & live events'),
(2, 1, 'Streaming platforms & online services'),
(3, 1, 'Marketplaces & e-commerce'),
(4, 1, 'Tech products & gadgets'),
(5, 1, 'Food & beverages'),
(6, 1, 'Travel & lifestyle services'),
(7, 1, 'Finance & banking apps'),
(8, 1, 'Health & fitness products'),
(9, 1, 'Mobile operators & internet providers'),
(10, 1, 'Education & online courses'),
(11, 2, 'Political satire'),
(12, 2, 'Social commentary & inequality'),
(13, 2, 'Human rights & activism'),
(14, 2, 'Immigration & migration'),
(15, 2, 'Law, crime & justice'),
(16, 2, 'Censorship & freedom of speech'),
(17, 3, 'Workplace humor & office culture'),
(18, 3, 'Money & personal finance'),
(19, 3, 'Career struggles & unemployment'),
(20, 3, 'Business & entrepreneurship'),
(21, 4, 'Physical health & fitness'),
(22, 4, 'Mental health & therapy'),
(23, 4, 'Addictions & substance use'),
(24, 4, 'Aging & body image'),
(25, 5, 'Dating & romance'),
(26, 5, 'Marriage & family life'),
(27, 5, 'Friendship & social circles'),
(28, 5, 'Sex & intimacy'),
(29, 5, 'Parenting'),
(30, 6, 'Tech & gadgets'),
(31, 6, 'Internet culture, memes & influencers'),
(32, 6, 'Artificial intelligence & future tech'),
(33, 6, 'Science news & discoveries'),
(34, 7, 'Movies, TV & streaming'),
(35, 7, 'Music & live performance'),
(36, 7, 'Literature & art references'),
(37, 7, 'Celebrities & fame'),
(38, 7, 'Pop culture trends'),
(39, 8, 'Climate change & sustainability jokes'),
(40, 8, 'Animals & pets'),
(41, 8, 'Urban vs rural life'),
(42, 8, 'Natural disasters & weather humor'),
(43, 9, 'Historical events satire'),
(44, 9, 'Cultural traditions & heritage'),
(45, 9, 'Generational differences & nostalgia'),
(46, 9, 'National stereotypes'),
(47, 10, 'Morbid comedy & death jokes'),
(48, 10, 'Offensive or taboo topics'),
(49, 10, 'Self-deprecating humor'),
(50, 10, 'Surreal or absurd comedy') ON CONFLICT (subcategory_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS standup_core.core_sound_features (
    video_id TEXT REFERENCES standup_core.core_videos (
        video_id
    ) ON DELETE CASCADE,
    sequence INT2,
    points INT2 NOT NULL,
    duration_seconds FLOAT4 NOT NULL,
    start_seconds FLOAT4 NOT NULL,
    end_seconds FLOAT4 NOT NULL,
    avg_confidence FLOAT4 NOT NULL,
    max_confidence FLOAT4 NOT NULL,
    PRIMARY KEY (video_id, sequence)
);

CREATE TABLE IF NOT EXISTS standup_core.core_chapters (
    video_id TEXT NOT NULL REFERENCES standup_core.core_videos (video_id),
    start_segment_id INT2,
    end_segment_id INT2,
    subcategory_id INT2 NOT NULL REFERENCES standup_core.core_subcategories (
        subcategory_id
    ) ON DELETE RESTRICT,
    PRIMARY KEY (video_id, start_segment_id, end_segment_id),
    FOREIGN KEY (
        video_id, start_segment_id
    ) REFERENCES standup_core.core_transcript_segments (
        video_id, segment_id
    ) ON DELETE CASCADE,
    FOREIGN KEY (
        video_id, end_segment_id
    ) REFERENCES standup_core.core_transcript_segments (
        video_id, segment_id
    ) ON DELETE CASCADE
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
