{{ config(
    schema='superset',
    materialized='view',
) }}

select
    dd.*,
    v.video_title,
    ch.channel_name,
    pl.playlist_title,
    sn.prev_day_view,
    sn.prev_day_like,
    sn.prev_day_comment
from {{ ref("fact_video_daily_snapshot") }} as sn
inner join {{ ref("dim_date") }} as dd on sn.date_id = dd.date_id
inner join {{ ref("dim_videos") }} as v on sn.video_id = v.video_id
inner join {{ ref("dim_channels") }} as ch on sn.channel_id = ch.channel_id
inner join {{ ref("dim_playlists") }} as pl on sn.playlist_id = pl.playlist_id
