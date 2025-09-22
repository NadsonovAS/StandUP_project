{{ config(materialized='table') }}

with segments as (
    select
        sp.video_id,
        (segment.key)::int as segment_id,
        nullif(segment.value ->> 'start', '')::numeric as start_s,
        nullif(segment.value ->> 'end', '')::numeric as end_s,
        segment.value ->> 'text' as text
    from {{ ref('stg_process_video') }} sp
    cross join lateral jsonb_each(coalesce(sp.transcribe_json, '{}'::jsonb)) as segment(key, value)
    where nullif(segment.key, '') is not null
)

select
    video_id,
    segment_id,
    start_s,
    end_s,
    text
from segments
where segment_id is not null
