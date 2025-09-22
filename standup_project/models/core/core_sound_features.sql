{{ config(materialized='table') }}

with laughs as (
    select
        sp.video_id,
        laugh.value
    from {{ ref('stg_process_video') }} sp
    cross join lateral jsonb_array_elements(coalesce(sp.sound_classifier_json -> 'laughs', '[]'::jsonb)) as laugh(value)
)

select
    video_id,
    nullif(value ->> 'time_offset', '')::numeric as time_offset,
    nullif(value ->> 'score', '')::numeric as score
from laughs
where nullif(value ->> 'time_offset', '') is not null
