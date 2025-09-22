{{ config(materialized='view') }}

with videos as (
    select * from {{ ref('core_videos') }}
),
chapters as (
    select * from {{ ref('core_llm_chapters') }}
),
classifications as (
    select * from {{ ref('core_llm_classifications') }}
),
transcript_segments as (
    select * from {{ ref('core_transcript_segments') }}
),
chapter_segments as (
    select
        ch.chapter_id,
        count(ts.segment_id) as segment_count
    from chapters ch
    left join transcript_segments ts
        on ch.video_id = ts.video_id
       and ts.segment_id between ch.start_segment_id and ch.end_segment_id
    group by ch.chapter_id
),
laughs as (
    select
        video_id,
        count(*) as laugh_events,
        sum(score) as total_laugh_score
    from {{ ref('core_sound_features') }}
    group by video_id
)

select
    v.video_id,
    v.video_title,
    v.video_url,
    v.channel_id,
    v.channel_name,
    v.playlist_id,
    v.playlist_title,
    v.duration,
    v.view_count,
    v.like_count,
    v.comment_count,
    v.upload_date,
    coalesce(l.laugh_events, 0) as laugh_events,
    coalesce(l.total_laugh_score, 0.0) as total_laugh_score,
    ch.chapter_id,
    ch.start_segment_id,
    ch.end_segment_id,
    coalesce(cs.segment_count, 0) as segment_count,
    ch.theme,
    ch.summary,
    cls.category_id,
    cls.main_category,
    cls.subcategory,
    cls.reason
from videos v
left join laughs l on v.video_id = l.video_id
left join chapters ch on v.video_id = ch.video_id
left join chapter_segments cs on ch.chapter_id = cs.chapter_id
left join classifications cls on ch.chapter_id = cls.chapter_id
