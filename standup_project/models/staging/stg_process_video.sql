{{ config(materialized='view') }}

with source_data as (
    select
        video_id,
        playlist_id,
        playlist_title,
        channel_id,
        channel_name,
        video_title,
        video_url,
        (video_meta_json ->> 'duration')::int as duration,
        (video_meta_json ->> 'like_count')::bigint as like_count,
        (video_meta_json ->> 'view_count')::bigint as view_count,
        (video_meta_json ->> 'comment_count')::int as comment_count,
        to_date(nullif(video_meta_json ->> 'upload_date', ''), 'YYYYMMDD') as upload_date,
        transcribe_json,
        llm_chapter_json,
        llm_classifier_json,
        sound_classifier_json,
        process_status
    from {{ source('standup_raw', 'process_video') }}
)

select *
from source_data
