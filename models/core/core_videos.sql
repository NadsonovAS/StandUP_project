{{ config (
    materialized='incremental',
    alias='videos'
) }}

with source_channels as (
    select video_id as yt_video_id, 
    ch.channel_id,
    pl.playlist_id,
    video_title,
    video_url,
    (video_meta_json ->> 'duration')::int2 as duration,
    (video_meta_json ->> 'like_count')::int as like_count,
    (video_meta_json ->> 'view_count')::int as view_count,
    (video_meta_json ->> 'comment_count')::int as comment_count,
    (video_meta_json ->> 'upload_date')::DATE as upload_date
    from {{ source('standup_raw','process_video') }} pr
    join {{ ref("core_channels") }} ch on ch.yt_channel_id =  pr.channel_id
    join {{ ref("core_playlists")}} pl on pl.yt_playlist_id =  pr.playlist_id
    where process_status = 'finished'
)


select nextval('standup_core.videos_video_id_seq') as video_id, *, current_timestamp as created_at
from source_channels sc
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.yt_video_id = sc.yt_video_id
)
{% endif %}