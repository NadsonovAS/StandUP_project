{{ config(
    schema='standup_mart',
    materialized='view',
    alias='time_of_chapters'
) }}

with CTE as (
select
	v.video_title,
	cat.main_category,
	sub.subcategory,
	seg_end.end_s - seg_start.start_s as duration,
    date_part('year', v.upload_date)::INT as "year"
from
	{{ref("core_chapters")}} as ch
join {{ref("core_transcript_segments")}} as seg_start on
	seg_start.video_id = ch.video_id
	and seg_start.segment_id = ch.start_segment_id
join {{ref("core_transcript_segments")}} as seg_end on
	seg_end.video_id = ch.video_id
	and seg_end.segment_id = ch.end_segment_id
join {{ref("core_videos")}} as v on
	v.video_id = ch.video_id
join {{source("standup_core", "subcategories")}} as sub on
	sub.subcategory_id = ch.subcategory_id
join {{source("standup_core", "categories")}} as cat on
	cat.category_id = sub.category_id)
select
	cte.main_category, cte.subcategory, cte.video_title,
	sum(cte.duration) / 3600 as total, cte."year"
from
	CTE
group by
	cte.main_category, cte.subcategory, cte.video_title, cte."year"