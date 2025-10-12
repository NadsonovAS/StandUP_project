{{ config(
    schema='standup_mart',
    materialized='view',
    alias='category_counter'
) }}

select cat.main_category, sub.subcategory, v.video_title, pl.playlist_title, date_part('year', v.upload_date)::INT as "year"
from {{ref("core_chapters")}} as ch
join {{source("standup_core", "subcategories")}} as sub on sub.subcategory_id = ch.subcategory_id
join {{source("standup_core", "categories")}} as cat on cat.category_id = sub.category_id
left join {{ ref("core_videos") }} as v on v.video_id = ch.video_id
left join {{ ref("core_playlists") }} as pl on pl.playlist_id = v.playlist_id