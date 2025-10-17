{{ config(
    materialized='view',
) }}

WITH raw_transcripts AS (
    SELECT 
        pr.video_id as video_id,
        pr.transcribe_json
    FROM {{ source('standup_raw', 'process_video') }} pr
    WHERE process_status = 'finished' AND transcribe_json IS NOT NULL
),

parsed_segments AS (
    SELECT
        video_id::TEXT,

        -- JSON extract with type
        (segment.key)::INT as segment_id,
        (segment.value ->> 'text')::TEXT as segment_text,
        (segment.value ->> 'start')::FLOAT as start_s,
        (segment.value ->> 'end')::FLOAT as end_s
        
FROM raw_transcripts
    CROSS JOIN LATERAL jsonb_each(transcribe_json) as segment
)

SELECT 
    video_id,
    segment_id,
    TRIM(segment_text) as segment_text,
    start_s,
    end_s,
    
    -- Validate
    CASE
        WHEN segment_text IS NULL OR segment_text = '' THEN FALSE
        WHEN end_s <= start_s THEN FALSE
        ELSE TRUE
    END as is_valid
    
FROM parsed_segments
