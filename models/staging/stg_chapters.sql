{{ config(
    materialized='view',
) }}

WITH process_video AS (
    SELECT
        video_id,
        llm_chapter_json
    FROM {{ source('standup_raw', 'process_video') }}
    WHERE process_status = 'finished' AND llm_chapter_json IS NOT NULL
),

parsed_chapters AS (
    SELECT
        video_id::TEXT AS video_id,
        ch.id AS start_segment_id,
        ch.end_id AS end_segment_id,
        ch.theme AS chapter_theme,
        ch.summary AS chapter_summary
    FROM process_video
    CROSS JOIN LATERAL jsonb_to_recordset(llm_chapter_json -> 'chapters')
        AS ch (id INT, end_id INT, theme TEXT, summary TEXT)
)

SELECT
    video_id,
    start_segment_id,
    end_segment_id,
    trim(chapter_theme) AS chapter_theme,
    trim(chapter_summary) AS chapter_summary,

    -- Validate
    CASE
        WHEN coalesce(chapter_theme, chapter_summary) IS NULL THEN FALSE
        WHEN end_segment_id < start_segment_id THEN FALSE
        ELSE TRUE
    END AS is_valid

FROM parsed_chapters
