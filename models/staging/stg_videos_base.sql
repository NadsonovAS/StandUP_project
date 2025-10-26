{{ config(
    materialized='view',
) }}

WITH process_video AS (
    SELECT
        video_id,
        channel_id,
        channel_name,
        playlist_id,
        playlist_title,
        video_title,
        video_url,
        video_meta_json,
        process_status,
        meta_updated_at
    FROM {{ source('standup_raw', 'process_video') }} pr
    WHERE pr.process_status = 'finished'
)

SELECT
    -- Youtube ID
    video_id::TEXT AS video_id,
    channel_id::TEXT AS channel_id,
    playlist_id::TEXT AS playlist_id,

    -- Name TEXT clean
    TRIM(video_title)::TEXT AS video_title,
    TRIM(channel_name)::TEXT AS channel_name,
    TRIM(playlist_title)::TEXT AS playlist_title,

    -- JSON extract with type
    (video_meta_json ->> 'duration')::INT AS duration,
    (video_meta_json ->> 'like_count')::INT AS like_count,
    (video_meta_json ->> 'view_count')::INT AS view_count,
    (video_meta_json ->> 'comment_count')::INT AS comment_count,
    (video_meta_json ->> 'upload_date')::DATE AS upload_date,

    -- Process meta field
    process_status::TEXT,
    meta_updated_at::DATE,

    -- Validate
    CASE
        WHEN video_id IS NULL OR video_id = '' THEN FALSE
        WHEN channel_id IS NULL OR channel_id = '' THEN FALSE
        WHEN playlist_id IS NULL OR playlist_id = '' THEN FALSE
        WHEN process_status != 'finished' OR process_status IS NULL THEN FALSE
        ELSE TRUE
    END AS is_valid

FROM process_video
