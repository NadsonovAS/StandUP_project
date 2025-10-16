{{ config(
    materialized='incremental',
    alias='playlists',
) }}

with stg_videos_base as (
    select distinct yt_playlist_id, playlist_title
    from {{ ref("stg_videos_base") }}
    where is_valid is true
)

select
    nextval('standup_core.playlists_playlist_id_seq') as playlist_id,
    yt_playlist_id,
    playlist_title,
    current_timestamp as created_at
from stg_videos_base stg_v
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.yt_playlist_id = stg_v.yt_playlist_id
)
{% endif %}
