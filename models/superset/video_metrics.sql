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
    d.date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week_of_year,
    d.day_of_week,
    d.day_name,
    d.is_weekend,
    to_char(make_interval(secs => vm.duration), 'HH24:MI:SS') as duration_hms
from {{ ref("fact_video_metrics") }} as vm
inner join {{ ref("dim_playlists") }} as pl on vm.playlist_id = pl.playlist_id
inner join {{ ref("dim_channels") }} as ch on vm.channel_id = ch.channel_id
inner join {{ ref("dim_videos") }} as v on vm.video_id = v.video_id
inner join {{ ref("dim_date") }} as d on vm.date_id = d.date_id
inner join {{ ref("fact_video_chapters") }} as chap on vm.video_id = chap.video_id
inner join
    {{ ref("dim_subcategory") }} as sub
    on chap.subcategory_id = sub.subcategory_id
inner join {{ ref("dim_category") }} as cat on chap.category_id = cat.category_id
