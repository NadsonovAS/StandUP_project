{{ config (
    unique_key=['video_id', 'sequence']
) }}

select
    stg_sf.video_id,
    stg_sf.sequence,
    stg_sf.points,
    stg_sf.duration_seconds,
    stg_sf.start_seconds,
    stg_sf.end_seconds,
    stg_sf.avg_confidence,
    stg_sf.max_confidence
from {{ ref("stg_sound_features") }} as stg_sf
where
    stg_sf.is_valid is TRUE
    {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where existing.video_id = stg_sf.video_id
        )
    {% endif %}
