{{ config (
    materialized='incremental',
    alias='videos_meta',
    unique_key=['video_id', 'snapshot_date']
) }}

with stg_videos_base as (
    select v.video_id,
    stg_v.meta_updated_at as snapshot_date,
    stg_v.like_count,
    stg_v.view_count,
    stg_v.comment_count
    from {{ ref("stg_videos_base") }} stg_v
    join {{ ref("core_videos") }} v on v.yt_video_id =  stg_v.yt_video_id
    where is_valid is true
)

select *
from stg_videos_base stg_v
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.video_id = stg_v.video_id and existing.snapshot_date >= stg_v.snapshot_date
)
{% endif %}