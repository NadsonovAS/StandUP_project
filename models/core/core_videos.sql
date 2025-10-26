{{ config (
    unique_key='video_id',
) }}

select
    stg_v.video_id,
    stg_v.channel_id,
    stg_v.playlist_id,
    stg_v.video_title,
    stg_v.duration,
    stg_v.upload_date,
    current_timestamp as created_at
from {{ ref("stg_videos_base") }} as stg_v
where
    stg_v.is_valid is true
    {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where existing.video_id = stg_v.video_id
        )
    {% endif %}
