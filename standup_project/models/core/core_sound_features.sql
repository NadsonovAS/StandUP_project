{{ config(
    materialized='incremental',
    alias='sound_features',
    on_schema_change='ignore'
) }}

with source_json as (
    select
        v.video_id,
        sp.sound_classifier_json,
        jsonb_typeof(sp.sound_classifier_json) as json_type
    from {{ ref('stg_process_video') }} sp
    join {{ ref('core_videos') }} v on v.video_id = sp.video_id
    where sp.sound_classifier_json is not null
      and jsonb_typeof(sp.sound_classifier_json) is not null
),
array_events as (
    select
        sj.video_id,
        nullif(event.value ->> 'time_offset', '') as time_offset_text,
        nullif(event.value ->> 'score', '') as score_text
    from source_json sj
    cross join lateral jsonb_array_elements(sj.sound_classifier_json) as event(value)
    where sj.json_type = 'array'
),
object_events as (
    select
        sj.video_id,
        nullif(event.key, '') as time_offset_text,
        (event.value)::text as score_text
    from source_json sj
    cross join lateral jsonb_each(sj.sound_classifier_json) as event(key, value)
    where sj.json_type = 'object'
),
combined as (
    select * from array_events
    union all
    select * from object_events
)

select
    c.video_id,
    c.time_offset_text::numeric as time_offset,
    c.score_text::numeric as score
from combined c
where c.time_offset_text is not null
  and c.score_text is not null
{% if is_incremental() %}
  and not exists (
        select 1
        from {{ this }} existing
        where existing.video_id = c.video_id
          and existing.time_offset = c.time_offset_text::numeric
    )
{% endif %}
