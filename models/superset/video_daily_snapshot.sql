{{ config(
    schema='superset',
    materialized='view',
) }}

select 
    v.video_title, 
    ch.channel_name,
    pl.playlist_title, 
    sn.prev_day_view, 
    sn.prev_day_like, 
    sn.prev_day_comment, 
    dd.*
from {{ref("fact_video_daily_snapshot")}} sn
join {{ref("dim_date")}} dd on dd.date_id = sn.date_id
join {{ref("dim_videos")}} v on v.video_id = sn.video_id
join {{ref("dim_channels")}} ch on ch.channel_id = sn.channel_id
join {{ref("dim_playlists")}} pl on pl.playlist_id = sn.playlist_id