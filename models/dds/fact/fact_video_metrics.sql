{{ config(
    schema='standup_dds',
    materialized='table',
    unique_key=['video_id'],
    post_hook=[
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_video 
       FOREIGN KEY (video_id) REFERENCES {{ ref('dim_videos') }} (video_id)",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_channel 
       FOREIGN KEY (channel_id) REFERENCES {{ ref('dim_channels') }} (channel_id)",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_playlist 
       FOREIGN KEY (playlist_id) REFERENCES {{ ref('dim_playlists') }} (playlist_id)",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_date
       FOREIGN KEY (date_id) REFERENCES {{ ref('dim_date') }} (date_id)"
    ]
) }}

with video_meta as (
    select distinct on (vm.video_id)
        vm.video_id,
        vm.like_count,
        vm.view_count,
        vm.comment_count,
        vm.snapshot_date
    from {{ref("core_videos_meta")}} as vm
    order by vm.video_id, vm.snapshot_date desc
),
laughter as (
    select sf.video_id, 
    round((sum(sf.duration_seconds) / cv.duration * 100)::numeric, 1) as laughter_percent
    from {{ref("core_sound_features")}} sf
    join {{ref("core_videos")}} cv on cv.video_id = sf.video_id
    where sf.duration_seconds > 1
    group by sf.video_id, cv.duration
)

select distinct
    cv.video_id,
    cv.playlist_id,
    cv.channel_id,
    dd.date_id,
    vm.view_count,
    vm.like_count,
    vm.comment_count,
    cv.duration,
    l.laughter_percent
from {{ref("core_videos")}} cv
join video_meta vm on vm.video_id = cv.video_id
join laughter l on l.video_id = cv.video_id
join {{ref("dim_date")}} dd on dd.date = cv.upload_date