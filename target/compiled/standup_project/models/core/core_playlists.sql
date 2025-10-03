

with source_playlists as (
    select distinct sp.playlist_id, sp.playlist_title
    from "standup_project"."standup_raw"."process_video" sp
    where sp.playlist_id is not null and sp.playlist_id <> ''
)

select
    nextval('standup_core.playlists_playlist_id_seq') as playlist_id,
    sp.playlist_id as yt_playlist_id,
    sp.playlist_title,
    current_timestamp as created_at
from source_playlists sp

where not exists (
    select 1
    from "standup_project"."standup_core"."playlists" existing
    where existing.yt_playlist_id = sp.playlist_id
)
