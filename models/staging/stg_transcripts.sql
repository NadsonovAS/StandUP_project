{{ config(
    materialized='view',
) }}

WITH raw_transcripts AS (
    SELECT
        pr.video_id,
        pr.transcribe_json
    FROM {{ source('standup_raw', 'process_video') }} AS pr
    WHERE pr.process_status = 'finished' AND pr.transcribe_json IS NOT NULL
),

parsed_segments AS (
    SELECT
        video_id::TEXT,

        -- JSON extract with type
        (segment.key)::INT AS segment_id,
        (segment.value ->> 'text')::TEXT AS segment_text,
        (segment.value ->> 'start')::FLOAT AS start_s,
        (segment.value ->> 'end')::FLOAT AS end_s

    FROM raw_transcripts
    CROSS JOIN LATERAL jsonb_each(transcribe_json) AS segment
)

SELECT
    video_id,
    segment_id,
    start_s,
    end_s,
    trim(segment_text) AS segment_text,

    -- Validate
    CASE
        WHEN segment_text IS NULL OR segment_text = '' THEN FALSE
        WHEN end_s <= start_s THEN FALSE
        ELSE TRUE
    END AS is_valid

FROM parsed_segments
