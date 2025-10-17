{{ config(
    schema='superset',
    materialized='view',
) }}

select 
    ch.channel_name,
    pl.playlist_title,
    v.video_title,
    sub.subcategory,
    cat.main_category,
    vm.view_count,
    vm.laughter_percent,
    vm.like_count,
    vm.comment_count,
    vm.duration,
    to_char(make_interval(secs => vm.duration), 'HH24:MI:SS') as duration_hms,
    d."date",
    d."year", 
    d.quarter, 
    d."month", 
    d.month_name, 
    d.week_of_year, 
    d.day_of_week, 
    d.day_name,
    d.is_weekend
from {{ref("fact_video_metrics")}} vm
join {{ref("dim_playlists")}} pl on pl.playlist_id = vm.playlist_id
join {{ref("dim_channels")}} ch on ch.channel_id = vm.channel_id
join {{ref("dim_videos")}} v on v.video_id = vm.video_id
join {{ref("dim_date")}} d on d.date_id = vm.date_id
join {{ref("fact_video_chapters")}} chap on chap.video_id = vm.video_id
join {{ref("dim_subcategory")}} sub on sub.subcategory_id = chap.subcategory_id
join {{ref("dim_category")}} cat on cat.category_id = chap.category_id