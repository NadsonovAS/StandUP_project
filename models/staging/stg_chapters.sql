{{ config(
    materialized='view',
    alias='stg_chapters'
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
        video_id::TEXT as yt_video_id,
        ch.id as start_segment_id,
        ch.end_id as end_segment_id,
        ch.theme as chapter_theme,
        ch.summary as chapter_summary
    FROM process_video
    CROSS JOIN LATERAL jsonb_to_recordset(llm_chapter_json -> 'chapters')
        as ch(id int, end_id int, theme text, summary text)
)

SELECT 
    yt_video_id,
    start_segment_id,
    end_segment_id,
    TRIM(chapter_theme) as chapter_theme,
    TRIM(chapter_summary) as chapter_summary,
    
    -- Validate
    CASE
        WHEN COALESCE(chapter_theme, chapter_summary) IS NULL THEN FALSE
        WHEN end_segment_id < start_segment_id THEN FALSE
        ELSE TRUE
    END as is_valid
    
FROM parsed_chapters