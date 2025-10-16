-- depends_on: {{ ref('core_transcript_segments') }}
-- depends_on: {{ ref('core_videos') }}

{{ config (
    materialized='incremental',
    alias='chapters'
) }}

with stg_chapters as (
	select
		yt_video_id,
		start_segment_id,
		end_segment_id
	from {{ ref("stg_chapters") }} stg_ch
	where is_valid is TRUE),
stg_classifications as (
	select
		yt_video_id,
		start_segment_id,
		main_category,
		subcategory
	from {{ ref("stg_classifications") }} stg_cl
	where is_valid is TRUE)
select
	cv.video_id,
	stg_ch.start_segment_id,
	stg_ch.end_segment_id,
	sub.subcategory_id
from
	stg_chapters stg_ch
join {{ref("core_videos")}} cv on
	cv.yt_video_id = stg_ch.yt_video_id
join stg_classifications stg_cl on
	stg_ch.yt_video_id = stg_cl.yt_video_id
	and stg_ch.start_segment_id = stg_cl.start_segment_id
join {{source('standup_core', 'subcategories')}} sub on
	sub.subcategory = stg_cl.subcategory
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.video_id = cv.video_id
)
{% endif %}