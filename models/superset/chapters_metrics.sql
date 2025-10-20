{{ config(
    schema='superset',
    materialized='view',
) }}

select
    cat.main_category,
    sub.subcategory,
    v.video_title,
    chan.channel_name,
    pl.playlist_title,
    ch.duration,
    ch.laughter_percent,
    d.date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week_of_year,
    d.day_of_week,
    d.day_name,
    d.is_weekend
from {{ ref("fact_video_chapters") }} as ch
inner join {{ ref("dim_category") }} as cat on ch.category_id = cat.category_id
inner join
    {{ ref("dim_subcategory") }} as sub
    on ch.subcategory_id = sub.subcategory_id
inner join {{ ref("dim_date") }} as d on ch.date_id = d.date_id
inner join {{ ref("dim_videos") }} as v on ch.video_id = v.video_id
inner join {{ ref("dim_channels") }} as chan on ch.channel_id = chan.channel_id
inner join {{ ref("dim_playlists") }} as pl on ch.playlist_id = pl.playlist_id
