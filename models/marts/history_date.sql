{{ config(
    schema='standup_mart',
    materialized='view',
    alias='history_date'
) }}

with CTE as (
select
	vm.video_id ,
	vm.snapshot_date ,
	(view_count - lag(view_count) over (partition by video_id
order by
	snapshot_date)) as prev_day_view,
	(like_count - lag(like_count) over (partition by video_id
order by
	snapshot_date)) as prev_day_like,
	(comment_count - lag(comment_count) over (partition by video_id
order by
	snapshot_date)) as prev_day_comment
from
	{{ref("core_videos_meta")}} as vm)
select
	cte.video_id,
	v.video_title,
    pl.playlist_title,
	cte.snapshot_date,
	sum(cte.prev_day_view) as prev_day_view,
	sum(cte.prev_day_like) as prev_day_like,
	sum(cte.prev_day_comment) as prev_day_comment
from
	CTE
join {{ref("core_videos")}} as v on
	v.video_id = cte.video_id
join {{ref("core_playlists")}} as pl on
    v.playlist_id = pl.playlist_id
group by
	cte.snapshot_date,
	cte.video_id,
	v.video_title,
    pl.playlist_title
order by
	cte.video_id,
	cte.snapshot_date