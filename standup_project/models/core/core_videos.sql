{{ config(materialized='table') }}

select
    video_id,
    playlist_id,
    playlist_title,
    channel_id,
    channel_name,
    video_title,
    video_url,
    duration,
    like_count,
    view_count,
    comment_count,
    upload_date,
    process_status
from {{ ref('stg_process_video') }}
