{{ config (
    materialized='incremental',
    unique_key='video_id'
) }}

select 
    stg_v.video_id,
    stg_v.channel_id,
    stg_v.playlist_id,
    video_title,
    duration,
    upload_date,
    current_timestamp as created_at
from {{ ref("stg_videos_base") }} stg_v
where is_valid is true and
{% if is_incremental() %}
    not exists (
    select 1
    from {{ this }} existing
    where existing.video_id = stg_v.video_id
)
{% endif %}