{{ config (
    materialized='incremental',
    alias='sound_features'
) }}

with stg_sound_features as (
    select 
        cv.video_id,
        stg_sf."sequence",
        stg_sf.points,
        stg_sf.duration_seconds,
        stg_sf.start_seconds,
        stg_sf.end_seconds,
        stg_sf.avg_confidence,
        stg_sf.max_confidence
    from {{ ref("stg_sound_features") }} stg_sf
    join {{ref("core_videos")}} cv on cv.yt_video_id = stg_sf.yt_video_id
    where stg_sf.is_valid is TRUE
)

select *
from stg_sound_features stg_sf
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.video_id = stg_sf.video_id
)
{% endif %}