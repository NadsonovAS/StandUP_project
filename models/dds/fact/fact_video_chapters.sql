{{ config(
    schema='standup_dds',
    materialized='table',
    unique_key=['video_id', 'subcategory_id', 'start_s'],
    post_hook=[
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_video 
       FOREIGN KEY (video_id) REFERENCES {{ ref('dim_videos') }} (video_id)",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_category
       FOREIGN KEY (category_id) REFERENCES {{ ref('dim_category') }} (category_id)",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_subcategory
       FOREIGN KEY (subcategory_id) REFERENCES {{ ref('dim_subcategory') }} (subcategory_id)",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_date
       FOREIGN KEY (date_id) REFERENCES {{ ref('dim_date') }} (date_id)"
    ]
) }}

with chapters as (
select
	ch.video_id,
	ch.subcategory_id,
	sub.category_id,
	tr1.start_s,
	tr2.end_s,
	round((tr2.end_s - tr1.start_s)::numeric, 2) as duration
from
		{{ref("core_chapters")}} ch
join {{source('standup_core', 'subcategories')}} sub on
		sub.subcategory_id = ch.subcategory_id
join {{ref("core_transcript_segments")}} tr1 on
		tr1.video_id = ch.video_id
	and tr1.segment_id = ch.start_segment_id
join {{ref("core_transcript_segments")}} tr2 on
		tr2.video_id = ch.video_id
	and tr2.segment_id = ch.end_segment_id)
select
	ch.video_id,
    dd.date_id,
    v.channel_id,
    v.playlist_id,
	ch.subcategory_id,
	ch.category_id,
	ch.start_s,
	ch.end_s,
	ch.duration,
	COALESCE(round((sum(sf.duration_seconds) / ch.duration * 100)::numeric, 1), 0) as laughter_percent
from
	chapters ch
left join {{ref("core_sound_features")}} sf on
	sf.video_id = ch.video_id
	and sf.start_seconds between ch.start_s and ch.end_s
join {{ref("core_videos")}} v on 
    v.video_id = ch.video_id
join {{ref("dim_date")}} dd on dd.date = v.upload_date
group by
	ch.video_id,
    dd.date_id,
    v.channel_id,
    v.playlist_id,
	ch.subcategory_id,
	ch.category_id,
	ch.start_s,
	ch.end_s,
	ch.duration
order by
	ch.video_id,
	ch.start_s