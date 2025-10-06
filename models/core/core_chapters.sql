-- depends_on: {{ ref('core_transcript_segments') }}
-- depends_on: {{ ref('core_videos') }}

{{ config (
    materialized='incremental',
    alias='chapters'
) }}

with chapter as (
select
	pr.video_id,
	ch.id,
	ch.end_id
from {{ source('standup_raw','process_video') }} pr
cross join lateral jsonb_to_recordset(pr.llm_chapter_json -> 'chapters')
    as ch(id int, end_id int, theme text, summary text)
where
	pr.process_status = 'finished'),
classifications as (
select
	pr.video_id ,
	cl.id,
	cl.main_category,
	cl.subcategory
from {{ source('standup_raw','process_video') }} pr
cross join lateral jsonb_to_recordset(pr.llm_classifier_json -> 'classifications')
	as cl (id int,
	main_category text,
	subcategory text,
	reason text)
where
	pr.process_status = 'finished')
select
	v.video_id,
	ch.id as start_segment_id,
	ch.end_id as end_segment_id,
	sub.subcategory_id
from
	chapter ch
join standup_core.videos v on
	v.yt_video_id = ch.video_id
join classifications cl on
	ch.video_id = cl.video_id
	and ch.id = cl.id
join standup_core.subcategories sub on
	sub.subcategory = cl.subcategory
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.video_id = v.video_id
)
{% endif %}