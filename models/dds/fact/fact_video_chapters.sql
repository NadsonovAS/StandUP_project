{{ config(
    schema='standup_dds',
    materialized='table',
    unique_key=['video_id', 'subcategory_id', 'start_s'],
    post_hook=[
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_video 
       FOREIGN KEY (video_id) REFERENCES {{ ref('dim_videos') }} (video_id)",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_category
       FOREIGN KEY (category_id) REFERENCES {{ ref('dim_category') }} (category_id)",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_subcategory
       FOREIGN KEY (subcategory_id) REFERENCES {{ ref('dim_subcategory') }} (subcategory_id)",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_date
       FOREIGN KEY (date_id) REFERENCES {{ ref('dim_date') }} (date_id)"
    ]
) }}

with chapters as (
    select
        ch.video_id,
        ch.subcategory_id,
        sub.category_id,
        tr1.start_s,
        tr2.end_s,
        round((tr2.end_s - tr1.start_s)::numeric, 2) as duration
    from
        {{ ref("core_chapters") }} as ch
        join {{ source('standup_core', 'core_subcategories') }} as sub
        on
            ch.subcategory_id = sub.subcategory_id
        join {{ ref("core_transcript_segments") }} as tr1
        on
            ch.video_id = tr1.video_id
            and ch.start_segment_id = tr1.segment_id
        join {{ ref("core_transcript_segments") }} as tr2 on
        ch.video_id = tr2.video_id
        and ch.end_segment_id = tr2.segment_id
)

select
    ch.video_id,
    dd.date_id,
    v.channel_id,
    v.playlist_id,
    ch.subcategory_id,
    ch.category_id,
    ch.start_s,
    ch.end_s,
    ch.duration,
    coalesce(
        round((sum(sf.duration_seconds) / ch.duration * 100)::numeric, 1), 0
    ) as laughter_percent
from
    chapters as ch
left join {{ ref("core_sound_features") }} as sf
    on
        ch.video_id = sf.video_id
        and sf.start_seconds between ch.start_s and ch.end_s
inner join {{ ref("core_videos") }} as v
    on
        ch.video_id = v.video_id
inner join {{ ref("dim_date") }} as dd on v.upload_date = dd.date
group by
    ch.video_id,
    dd.date_id,
    v.channel_id,
    v.playlist_id,
    ch.subcategory_id,
    ch.category_id,
    ch.start_s,
    ch.end_s,
    ch.duration
order by
    ch.video_id,
    ch.start_s
