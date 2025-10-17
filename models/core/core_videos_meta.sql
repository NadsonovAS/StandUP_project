{{ config (
    materialized='incremental',
    unique_key=['video_id', 'snapshot_date']
) }}

select 
    stg_v.video_id,
    stg_v.meta_updated_at as snapshot_date,
    stg_v.like_count,
    stg_v.view_count,
    stg_v.comment_count
from {{ ref("stg_videos_base") }} stg_v
where is_valid is true and
{% if is_incremental() %}
    not exists (
    select 1
    from {{ this }} existing
    where existing.video_id = stg_v.video_id and existing.snapshot_date >= stg_v.meta_updated_at
)
{% endif %}