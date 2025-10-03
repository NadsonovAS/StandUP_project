

with source_channels as (
    select 
        v.video_id,
        (j.key)::float  as time_offset,
        (j.value)::float as score
    from "standup_project"."standup_raw"."process_video" pr
    cross join lateral jsonb_each_text(pr.sound_classifier_json) as j(key, value)
    join "standup_project"."standup_core"."videos" as v on v.yt_video_id = pr.video_id
)

select *
from source_channels sc

where not exists (
    select 1
    from "standup_project"."standup_core"."sound_features" existing
    where existing.video_id = sc.video_id
)
