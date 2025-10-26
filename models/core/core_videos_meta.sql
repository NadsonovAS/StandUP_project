{{ config (
    unique_key=['video_id', 'snapshot_date']
) }}

select
    stg_v.video_id,
    stg_v.meta_updated_at as snapshot_date,
    stg_v.like_count,
    stg_v.view_count,
    stg_v.comment_count
from {{ ref("stg_videos_base") }} as stg_v
where
    stg_v.is_valid is true
    {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where
                existing.video_id = stg_v.video_id
                and existing.snapshot_date >= stg_v.meta_updated_at
        )
    {% endif %}
