-- depends_on: {{ ref('core_chapters') }}

{{ config(
    materialized='incremental',
    alias='classifications'
) }}

with raw_classifications as (
    select
        pv.video_id as yt_video_id,
        cls_record.id::int as chapter_segment_id,
        cls_record.main_category::text as main_category,
        cls_record.subcategory::text as subcategory,
        cls_record.reason::text as reason
    from {{ source('standup_raw', 'process_video') }} pv
    cross join lateral jsonb_to_recordset(
        coalesce(pv.llm_classifier_json -> 'classifications', '[]'::jsonb)
    ) as cls_record(
        id int,
        main_category text,
        subcategory text,
        reason text
    )
    where pv.process_status = 'finished'
), mapped_classifications as (
    select
        v.video_id,
        ch.chapter_id,
        cat.category_id,
        sub.subcategory_id,
        rc.reason
    from raw_classifications rc
    join {{ ref('core_videos') }} v on v.yt_video_id = rc.yt_video_id
    join {{ ref('core_chapters') }} ch
        on ch.video_id = v.video_id
        and ch.start_segment_id = rc.chapter_segment_id
    join {{ source('standup_core', 'categories') }} cat
        on cat.main_category = rc.main_category
    join {{ source('standup_core', 'subcategories') }} sub
        on sub.category_id = cat.category_id
        and sub.subcategory = rc.subcategory
)

select
    nextval('standup_core.classifications_classification_id_seq') as classification_id,
    mapped_classifications.chapter_id,
    mapped_classifications.category_id,
    mapped_classifications.subcategory_id,
    mapped_classifications.reason
from mapped_classifications
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.chapter_id = mapped_classifications.chapter_id
)
{% endif %}
