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
        video_id::TEXT as yt_video_id,
	    le.*
    FROM process_video
    cross join lateral jsonb_to_recordset(laugh_events_json -> 'events')
        as le ("sequence" int,
        points int,
        duration_seconds float,
        start_seconds float,
        end_seconds float,
        avg_confidence float,
        max_confidence float)
)

SELECT 
    yt_video_id,
    "sequence",
	points,
	duration_seconds,
	start_seconds,
	end_seconds,
	avg_confidence,
	max_confidence,
    
    -- Validate
    CASE
        WHEN COALESCE(duration_seconds, avg_confidence, max_confidence, start_seconds, end_seconds, points) IS NULL THEN FALSE
        WHEN duration_seconds <= 0 THEN FALSE
        WHEN avg_confidence <= 0 OR avg_confidence > 1 THEN FALSE
        ELSE TRUE
    END as is_valid
    
FROM parsed_features