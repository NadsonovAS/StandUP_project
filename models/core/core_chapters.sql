-- depends_on: {{ ref('core_transcript_segments') }}
-- depends_on: {{ ref('core_videos') }}

{{ config (
    materialized='incremental',
    alias='chapters'
) }}

with source_channels as (
    SELECT
        scv.video_id,
        ch.id as start_segment_id,
        ch.end_id as end_segment_id,
        ch.theme,
        ch.summary
    from {{ source('standup_raw','process_video') }} pr
    cross join lateral jsonb_to_recordset(pr.llm_chapter_json -> 'chapters')
        as ch(id int, end_id int, theme text, summary text)
    JOIN standup_core.videos scv ON scv.yt_video_id = pr.video_id
    where pr.process_status = 'finished'
)

select nextval('standup_core.chapters_chapter_id_seq') as chapter_id, *
from source_channels sc
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.video_id = sc.video_id
)
{% endif %}
