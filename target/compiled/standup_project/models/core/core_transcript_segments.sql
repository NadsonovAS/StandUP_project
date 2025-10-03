

with source_channels as (
    select
        v.video_id , 
        j.key::int4 as segment_id,
        (j.value ->> 'text')::text AS "text", 
        (j.value ->> 'start')::float AS "start_s", 
        (j.value ->> 'end')::float AS "end_s"
    from "standup_project"."standup_raw"."process_video" pr
    cross join lateral jsonb_each(pr.transcribe_json) as j(key, value)
    join "standup_project"."standup_core"."videos" as v on v.yt_video_id = pr.video_id
)

select *
from source_channels sc

where not exists (
    select 1
    from "standup_project"."standup_core"."transcript_segments" existing
    where existing.video_id = sc.video_id
)
