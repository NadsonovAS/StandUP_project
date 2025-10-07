{{ config(
    schema='standup_mart',
    materialized='view',
    alias='video_meta_info'
) }}

with chapter_categories as (
    select distinct
        ch.video_id,
        cat.main_category,
        sub.subcategory
    from {{ ref('core_chapters') }} as ch
    join {{ source('standup_core', 'subcategories') }} as sub on sub.subcategory_id = ch.subcategory_id
    join {{ source('standup_core', 'categories') }} as cat on cat.category_id    = sub.category_id
),
aggregated_laughter as (
    select
        cs.video_id,
        round((sum(cs.laughter_count) * 0.2 / sum(cs.duration) * 100)::numeric, 1) as laughter_percent
    from (
        select
            ch.video_id,
            (seg_end.end_s - seg_start.start_s) as duration,
            count(sf.time_offset) as laughter_count
        from {{ ref('core_chapters') }} as ch
        join {{ref("core_transcript_segments")}} as seg_start
          on seg_start.video_id = ch.video_id and seg_start.segment_id = ch.start_segment_id
        join {{ref("core_transcript_segments")}} as seg_end
          on seg_end.video_id = ch.video_id and seg_end.segment_id = ch.end_segment_id
        left join {{ref("core_sound_features")}} as sf
          on sf.video_id = ch.video_id and sf.time_offset between seg_start.start_s and seg_end.end_s
        group by ch.video_id, duration
    ) cs
    group by cs.video_id
)
select distinct
    ch.channel_name,
    pl.playlist_title,
    v.video_id,
    v.video_title,
    cc.main_category,
    cc.subcategory,
    al.laughter_percent,
    v.view_count,
    v.like_count,
    v.comment_count,
    to_char(make_interval(secs => v.duration), 'HH24:MI:SS') as duration_hms,
    v.upload_date
from {{ref("core_videos")}} as v
join {{ref("core_playlists")}} as pl on pl.playlist_id = v.playlist_id
join {{ref('core_channels')}} as ch on ch.channel_id = v.channel_id
left join aggregated_laughter al on al.video_id = v.video_id
join chapter_categories cc on cc.video_id = v.video_id