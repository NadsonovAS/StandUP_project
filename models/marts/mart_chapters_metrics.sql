{{ config(
    schema='standup_mart',
    materialized='view',
    alias='chapters_metrics'
) }}

select
    cat.main_category,
    sub.subcategory,
    v.video_title,
    chan.channel_name,
    pl.playlist_title,
    ch.duration,
    ch.laughter_percent,
    d."date",
    d."year", 
    d.quarter, 
    d."month", 
    d.month_name, 
    d.week_of_year, 
    d.day_of_week, 
    d.day_name,
    d.is_weekend
from {{ref("fact_video_chapters")}} ch
join {{ref("dim_category")}} cat on cat.category_id = ch.category_id
join {{ref("dim_subcategory")}} sub on sub.subcategory_id = ch.subcategory_id
join {{ref("dim_date")}} d on d.date_id = ch.date_id
join {{ref("dim_videos")}} v on v.video_id = ch.video_id
join {{ref("dim_channels")}} chan on chan.channel_id = ch.channel_id
join {{ref("dim_playlists")}} pl on pl.playlist_id= ch.playlist_id
