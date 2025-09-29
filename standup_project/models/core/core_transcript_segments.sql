{{ config(
    materialized='incremental',
    alias='transcript_segments',
    on_schema_change='ignore'
) }}

with segments as (
    select
        v.video_id,
        (segment.key)::int as segment_id,
        nullif(segment.value ->> 'start', '')::numeric as start_s,
        nullif(segment.value ->> 'end', '')::numeric as end_s,
        segment.value ->> 'text' as text
    from {{ ref('stg_process_video') }} sp
    join {{ ref('core_videos') }} v on v.video_id = sp.video_id
    cross join lateral jsonb_each(
        case
            when sp.transcribe_json is not null
             and jsonb_typeof(sp.transcribe_json) = 'object'
                then sp.transcribe_json
            else '{}'::jsonb
        end
    ) as segment(key, value)
    where nullif(segment.key, '') is not null
)

select
    s.video_id,
    s.segment_id,
    s.start_s,
    s.end_s,
    s.text
from segments s
where s.segment_id is not null
{% if is_incremental() %}
  and not exists (
        select 1
        from {{ this }} existing
        where existing.video_id = s.video_id
          and existing.segment_id = s.segment_id
    )
{% endif %}
