

with source_channels as (
    select video_id as yt_video_id, 
    ch.channel_id,
    pl.playlist_id,
    video_title,
    video_url,
    (video_meta_json ->> 'duration')::int2 as duration,
    (video_meta_json ->> 'like_count')::int as like_count,
    (video_meta_json ->> 'view_count')::int as view_count,
    (video_meta_json ->> 'comment_count')::int as comment_count,
    (video_meta_json ->> 'upload_date')::DATE as upload_date
    from "standup_project"."standup_raw"."process_video" pr
    join "standup_project"."standup_core"."channels" ch on ch.yt_channel_id =  pr.channel_id
    join "standup_project"."standup_core"."playlists" pl on pl.yt_playlist_id =  pr.playlist_id
    where process_status = 'finished'
)


select nextval('standup_core.videos_video_id_seq') as video_id, *
from source_channels sc

where not exists (
    select 1
    from "standup_project"."standup_core"."videos" existing
    where existing.yt_video_id = sc.yt_video_id
)
