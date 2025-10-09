{{ config (
    materialized='incremental',
    alias='videos_meta',
    unique_key=['video_id', 'snapshot_date']
) }}

with source_channels as (
    select v.video_id,
    pr.meta_updated_at as snapshot_date,
    (video_meta_json ->> 'like_count')::int as like_count,
    (video_meta_json ->> 'view_count')::int as view_count,
    (video_meta_json ->> 'comment_count')::int as comment_count
    from {{ source('standup_raw','process_video') }} pr
    join {{ ref("core_videos") }} v on v.yt_video_id =  pr.video_id
    where process_status = 'finished'
)

select *
from source_channels sc
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.video_id = sc.video_id and existing.snapshot_date >= sc.snapshot_date
)
{% endif %}