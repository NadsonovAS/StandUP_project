{{ config(
    unique_key=['video_id', 'date_id']
) }}

with videos_meta as (
    select
        vm.video_id,
        vm.snapshot_date,
        (vm.view_count - lag(vm.view_count) over (
            partition by vm.video_id
            order by
                vm.snapshot_date
        )) as prev_day_view,
        (vm.like_count - lag(vm.like_count) over (
            partition by vm.video_id
            order by
                vm.snapshot_date
        )) as prev_day_like,
        (vm.comment_count - lag(vm.comment_count) over (
            partition by vm.video_id
            order by
                vm.snapshot_date
        )) as prev_day_comment
    from
        {{ ref("core_videos_meta") }} as vm
)

select
    vm.video_id,
    dd.date_id,
    v.channel_id,
    v.playlist_id,
    sum(vm.prev_day_view) as prev_day_view,
    sum(vm.prev_day_like) as prev_day_like,
    sum(vm.prev_day_comment) as prev_day_comment
from
    videos_meta as vm
inner join {{ ref("core_videos") }} as v
    on
        vm.video_id = v.video_id
inner join {{ ref("dim_date") }} as dd on vm.snapshot_date = dd.date
group by
    dd.date_id,
    vm.video_id,
    v.channel_id,
    v.playlist_id
