{{ config(materialized='table') }}

with chapters as (
    select
        sp.video_id,
        concat_ws('_', sp.video_id, chapter.value ->> 'id') as chapter_id,
        (chapter.value ->> 'id')::int as start_segment_id,
        (chapter.value ->> 'end_id')::int as end_segment_id,
        chapter.value ->> 'theme' as theme,
        chapter.value ->> 'summary' as summary
    from {{ ref('stg_process_video') }} sp
    cross join lateral jsonb_array_elements(coalesce(sp.llm_chapter_json -> 'chapters', '[]'::jsonb)) as chapter(value)
    where nullif(chapter.value ->> 'id', '') is not null
)

select
    video_id,
    chapter_id,
    start_segment_id,
    end_segment_id,
    theme,
    summary
from chapters
where chapter_id is not null
