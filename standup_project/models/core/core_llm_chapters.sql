{{ config(
    materialized='incremental',
    alias='llm_chapters',
    on_schema_change='ignore'
) }}

with raw_chapters as (
    select
        v.video_id,
        (chapter.value ->> 'id')::int as start_segment_id,
        (chapter.value ->> 'end_id')::int as end_segment_id,
        nullif(chapter.value ->> 'theme', '') as theme,
        nullif(chapter.value ->> 'summary', '') as summary
    from {{ ref('stg_process_video') }} sp
    join {{ ref('core_videos') }} v on v.video_id = sp.video_id
    cross join lateral jsonb_array_elements(
        coalesce(sp.llm_chapter_json -> 'chapters', '[]'::jsonb)
    ) as chapter(value)
    where nullif(chapter.value ->> 'id', '') is not null
)
,
dedup_chapters as (
    select distinct
        video_id,
        start_segment_id,
        end_segment_id,
        theme,
        summary
    from raw_chapters
    where start_segment_id is not null and end_segment_id is not null
)
,
validated_chapters as (
    select
        dc.video_id,
        dc.start_segment_id,
        dc.end_segment_id,
        dc.theme,
        dc.summary
    from dedup_chapters dc
    join {{ ref('core_transcript_segments') }} start_seg
        on start_seg.video_id = dc.video_id
       and start_seg.segment_id = dc.start_segment_id
    join {{ ref('core_transcript_segments') }} end_seg
        on end_seg.video_id = dc.video_id
       and end_seg.segment_id = dc.end_segment_id
),
existing as (
    {% if is_incremental() %}
    select chapter_id, video_id, start_segment_id, end_segment_id
    from {{ this }}
    {% else %}
    select null::int as chapter_id, null::text as video_id, null::int as start_segment_id, null::int as end_segment_id
    where false
    {% endif %}
),
new_chapters as (
    select
        vc.video_id,
        vc.start_segment_id,
        vc.end_segment_id,
        vc.theme,
        vc.summary
    from validated_chapters vc
    left join existing e
      on e.video_id = vc.video_id
     and e.start_segment_id = vc.start_segment_id
     and e.end_segment_id = vc.end_segment_id
    where e.chapter_id is null
),
numbered_new as (
    select
        nc.video_id,
        nc.start_segment_id,
        nc.end_segment_id,
        nc.theme,
        nc.summary,
        row_number() over (
            order by nc.video_id, nc.start_segment_id, nc.end_segment_id
        ) as new_rank
    from new_chapters nc
),
max_existing as (
    select coalesce(max(chapter_id), 0) as max_id from existing
)

select
    max_existing.max_id + numbered_new.new_rank as chapter_id,
    numbered_new.video_id,
    numbered_new.start_segment_id,
    numbered_new.end_segment_id,
    numbered_new.theme,
    numbered_new.summary
from numbered_new
cross join max_existing
