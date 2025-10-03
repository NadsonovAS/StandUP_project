

with source_channels as (
    select distinct sp.channel_id, sp.channel_name
    from "standup_project"."standup_raw"."process_video" sp
    where sp.channel_id is not null
)

select
    nextval('standup_core.channels_channel_id_seq') as channel_id,
    sc.channel_id as yt_channel_id,
    sc.channel_name,
    current_timestamp as created_at
from source_channels sc

where not exists (
    select 1
    from "standup_project"."standup_core"."channels" existing
    where existing.yt_channel_id = sc.channel_id
)
