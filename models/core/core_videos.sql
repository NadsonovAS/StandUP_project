{{ config (
    materialized='incremental',
    alias='videos'
) }}

with stg_videos_base as (
    select stg_v.yt_video_id,
    ch.channel_id,
    pl.playlist_id,
    video_title,
    duration,
    upload_date
    from {{ ref("stg_videos_base") }} stg_v
    join {{ ref("core_channels") }} ch on ch.yt_channel_id =  stg_v.yt_channel_id
    join {{ ref("core_playlists")}} pl on pl.yt_playlist_id =  stg_v.yt_playlist_id
    where is_valid is true
)


select nextval('standup_core.videos_video_id_seq') as video_id, *, current_timestamp as created_at
from stg_videos_base stg_v
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.yt_video_id = stg_v.yt_video_id
)
{% endif %}