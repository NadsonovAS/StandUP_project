{{ config(
    unique_key=['video_id'],
) }}

with video_meta as (
    select distinct on (vm.video_id)
        vm.video_id,
        vm.like_count,
        vm.view_count,
        vm.comment_count,
        vm.snapshot_date
    from {{ ref("core_videos_meta") }} as vm
    order by vm.video_id asc, vm.snapshot_date desc
),

laughter as (
    select
        sf.video_id,
        round((sum(sf.duration_seconds) / cv.duration * 100)::numeric, 1)
            as laughter_percent
    from {{ ref("core_sound_features") }} as sf
    inner join {{ ref("core_videos") }} as cv on sf.video_id = cv.video_id
    where sf.duration_seconds > 1
    group by sf.video_id, cv.duration
)

select distinct
    cv.video_id,
    cv.playlist_id,
    cv.channel_id,
    dd.date_id,
    vm.view_count,
    vm.like_count,
    vm.comment_count,
    cv.duration,
    l.laughter_percent
from {{ ref("core_videos") }} as cv
inner join video_meta as vm on cv.video_id = vm.video_id
inner join laughter as l on cv.video_id = l.video_id
inner join {{ ref("dim_date") }} as dd on cv.upload_date = dd.date
