{{ config(
    materialized='view',
    alias='stg_videos_base'
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
    FROM {{ source('standup_raw', 'process_video') }}
)

SELECT
    -- Youtube ID
    video_id::TEXT as yt_video_id,
    channel_id::TEXT as yt_channel_id,
    playlist_id::TEXT as yt_playlist_id,
    
    -- Name TEXT clean
    TRIM(video_title)::TEXT as video_title,
    TRIM(channel_name)::TEXT as channel_name,
    TRIM(playlist_title)::TEXT as playlist_title,
    
    -- JSON extract with type
    (video_meta_json ->> 'duration')::INT as duration,
    (video_meta_json ->> 'like_count')::INT as like_count,
    (video_meta_json ->> 'view_count')::INT as view_count,
    (video_meta_json ->> 'comment_count')::INT as comment_count,
    (video_meta_json ->> 'upload_date')::DATE as upload_date,
    
    -- Process meta field
    process_status::TEXT,
    meta_updated_at::DATE,
    
    -- Validate
    CASE 
        WHEN video_id IS NULL OR video_id = '' THEN FALSE
        WHEN channel_id IS NULL OR channel_id = '' THEN FALSE
        WHEN playlist_id IS NULL OR playlist_id = '' THEN FALSE
        WHEN process_status != 'finished' or process_status IS NULL THEN FALSE
        ELSE TRUE
    END as is_valid
    
FROM process_video