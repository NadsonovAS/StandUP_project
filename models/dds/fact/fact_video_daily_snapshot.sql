{{ config(
    schema='standup_dds',
    materialized='table',
    unique_key=['video_id', 'date_id'],
    post_hook=[
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_video 
       FOREIGN KEY (video_id) REFERENCES {{ ref('dim_videos') }} (video_id)",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_date
       FOREIGN KEY (date_id) REFERENCES {{ ref('dim_date') }} (date_id)",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_channel 
       FOREIGN KEY (channel_id) REFERENCES {{ ref('dim_channels') }} (channel_id)",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_playlist 
       FOREIGN KEY (playlist_id) REFERENCES {{ ref('dim_playlists') }} (playlist_id)"
    ]
) }}

with videos_meta as (
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
	vm.video_id,
	dd.date_id,
    v.channel_id,
    v.playlist_id,
	sum(vm.prev_day_view) as prev_day_view,
	sum(vm.prev_day_like) as prev_day_like,
	sum(vm.prev_day_comment) as prev_day_comment
from
	videos_meta vm
join {{ref("core_videos")}} as v on
	v.video_id = vm.video_id
join {{ref("core_playlists")}} as pl on
    v.playlist_id = pl.playlist_id
join {{ref("dim_date")}} dd on dd.date = vm.snapshot_date
group by
	dd.date_id,
	vm.video_id,
    v.channel_id,
    v.playlist_id