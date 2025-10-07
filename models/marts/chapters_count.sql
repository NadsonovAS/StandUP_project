{{ config(
    schema='standup_mart',
    materialized='view',
    alias='category_counter'
) }}

select cat.main_category, sub.subcategory, v.video_title, pl.playlist_title
from {{ref("core_chapters")}} as ch
join standup_core.subcategories sub on sub.subcategory_id = ch.subcategory_id
join standup_core.categories cat on cat.category_id = sub.category_id
left join standup_core.videos v on v.video_id = ch.video_id
left join standup_core.playlists pl on pl.playlist_id = v.playlist_id