{{ config(
    materialized='view',
) }}

WITH process_video AS (
    SELECT
        video_id,
        laugh_events_json
    FROM {{ source('standup_raw', 'process_video') }}
    WHERE process_status = 'finished' AND laugh_events_json IS NOT NULL
),

parsed_features AS (
    SELECT
        le.*,
        video_id::TEXT AS video_id
    FROM process_video
    CROSS JOIN LATERAL jsonb_to_recordset(laugh_events_json -> 'events')
        AS le (
            "sequence" INT,
            points INT,
            duration_seconds FLOAT,
            start_seconds FLOAT,
            end_seconds FLOAT,
            avg_confidence FLOAT,
            max_confidence FLOAT
        )
)

SELECT
    video_id,
    sequence,
    points,
    duration_seconds,
    start_seconds,
    end_seconds,
    avg_confidence,
    max_confidence,

    -- Validate
    CASE
        WHEN
            coalesce(
                duration_seconds,
                avg_confidence,
                max_confidence,
                start_seconds,
                end_seconds,
                points
            ) IS NULL
            THEN FALSE
        WHEN duration_seconds <= 0 THEN FALSE
        WHEN avg_confidence <= 0 OR avg_confidence > 1 THEN FALSE
        ELSE TRUE
    END AS is_valid

FROM parsed_features
