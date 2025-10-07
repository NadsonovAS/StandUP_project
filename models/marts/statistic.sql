{{ config(
    schema='standup_mart',
    materialized='view',
    alias='dwh_statistics'
) }}

select
	'1. Всего каналов' as label,
	COUNT(channel_id) as total
from
	{{ref("core_channels")}}
union
select
	'2. Всего плейлистов' as label,
	COUNT(playlist_id) as total
from
	{{ref("core_playlists")}}
union
select
	'3. Всего видео' as label,
	count(video_id) as total
from
	{{ref("core_videos")}}
union
select
	'4. Всего часов' as label,
	sum(duration) / 3600 as total
from
	{{ref("core_videos")}}