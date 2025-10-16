{{ config (
    materialized='incremental',
    alias='transcript_segments'
) }}

with stg_transcripts as (
    select
        v.video_id , 
        stg_tr.yt_video_id,
        stg_tr.segment_id,
        stg_tr.segment_text,
        stg_tr.start_s,
        stg_tr.end_s
    from {{ ref("stg_transcripts") }} stg_tr
    join {{ref("core_videos")}} as v on v.yt_video_id = stg_tr.yt_video_id
    where stg_tr.is_valid is TRUE
)

select *
from stg_transcripts stg_tr
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.video_id = stg_tr.video_id
)
{% endif %}
