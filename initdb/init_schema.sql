-- initdb/init_schema.sql
-- 1) Create DWH schemas
CREATE SCHEMA IF NOT EXISTS standup_raw;

CREATE SCHEMA IF NOT EXISTS standup_core;

CREATE SCHEMA IF NOT EXISTS standup_mart;

-- 2) Create Table
-- RAW Tables
CREATE TABLE IF NOT EXISTS standup_raw.process_video (
    channel_id varchar(25),
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
CREATE TABLE IF NOT EXISTS standup_core.channels (
    channel_id serial PRIMARY KEY,
    yt_channel_id text UNIQUE NOT NULL,
    channel_name text NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now ()
);

CREATE TABLE IF NOT EXISTS standup_core.playlists (
    playlist_id serial PRIMARY KEY,
    yt_playlist_id text UNIQUE NOT NULL,
    playlist_title text,
    created_at TIMESTAMPTZ DEFAULT now ()
);

CREATE TABLE IF NOT EXISTS standup_core.videos (
    video_id serial PRIMARY KEY,
    yt_video_id text UNIQUE NOT NULL,
    playlist_id int REFERENCES standup_core.playlists (playlist_id),
    channel_id int REFERENCES standup_core.channels (channel_id),
    video_title text,
    duration int2,
    like_count int4,
    view_count int4,
    comment_count int4,
    upload_date DATE,
    created_at TIMESTAMPTZ DEFAULT now ()
);

CREATE TABLE IF NOT EXISTS standup_core.transcript_segments (
    video_id int REFERENCES standup_core.videos (video_id) ON DELETE CASCADE,
    segment_id int4,
    start_s float4,
    end_s float4,
    text TEXT,
    PRIMARY KEY (video_id, segment_id)
);


CREATE TABLE IF NOT EXISTS standup_core.categories (
    category_id int2 PRIMARY KEY,
    main_category text UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS standup_core.subcategories (
    subcategory_id int2 PRIMARY KEY,
    category_id int2 NOT NULL REFERENCES standup_core.categories (category_id) ON DELETE CASCADE,
    subcategory text NOT NULL,
    UNIQUE (category_id, subcategory)
);

-- Seed lookup data for LLM categories and subcategories
INSERT INTO
    standup_core.categories (category_id, main_category)
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
    standup_core.subcategories (subcategory_id, category_id, subcategory)
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

CREATE TABLE IF NOT EXISTS standup_core.sound_features (
    video_id int REFERENCES standup_core.videos (video_id) ON DELETE CASCADE,
    time_offset float4,
    score float4,
    PRIMARY KEY (video_id, time_offset)
);

CREATE TABLE IF NOT EXISTS standup_core.chapters (
    video_id int NOT NULL REFERENCES standup_core.videos (video_id),
    start_segment_id int2 NOT NULL,
    end_segment_id int2 NOT NULL,
    subcategory_id int2 REFERENCES standup_core.subcategories (subcategory_id) ON DELETE RESTRICT,
    UNIQUE (video_id, start_segment_id, end_segment_id),
    FOREIGN KEY (video_id, start_segment_id) REFERENCES standup_core.transcript_segments (video_id, segment_id) ON DELETE CASCADE,
    FOREIGN KEY (video_id, end_segment_id) REFERENCES standup_core.transcript_segments (video_id, segment_id) ON DELETE CASCADE
);