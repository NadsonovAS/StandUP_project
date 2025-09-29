{{ config(
    materialized='incremental',
    alias='videos',
    on_schema_change='ignore',
    incremental_strategy='delete+insert',
    unique_key='video_id'
) }}

with source_videos as (
    select
        nullif(sp.video_id, '') as video_id,
        nullif(sp.playlist_id, '') as playlist_id,
        nullif(sp.channel_id, '') as channel_id,
        sp.video_title,
        sp.video_url,
        sp.duration,
        sp.like_count,
        sp.view_count,
        sp.comment_count,
        sp.upload_date
    from {{ ref('stg_process_video') }} sp
    where sp.video_id is not null and sp.video_id <> ''
)

select
    sv.video_id,
    sv.playlist_id,
    sv.channel_id,
    sv.video_title,
    sv.video_url,
    sv.duration,
    sv.like_count,
    sv.view_count,
    sv.comment_count,
    sv.upload_date,
    current_timestamp as created_at
from source_videos sv
