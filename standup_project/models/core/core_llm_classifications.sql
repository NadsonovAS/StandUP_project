{{ config(
    materialized='incremental',
    alias='llm_classifications',
    on_schema_change='ignore'
) }}

with raw_classifications as (
    select
        v.video_id,
        (classification.value ->> 'id')::int as start_segment_id,
        nullif(btrim(classification.value ->> 'main_category'), '') as main_category,
        nullif(btrim(classification.value ->> 'subcategory'), '') as subcategory,
        nullif(classification.value ->> 'reason', '') as reason
    from {{ ref('stg_process_video') }} sp
    join {{ ref('core_videos') }} v on v.video_id = sp.video_id
    cross join lateral jsonb_array_elements(
        coalesce(sp.llm_classifier_json -> 'classifications', '[]'::jsonb)
    ) as classification(value)
    where nullif(classification.value ->> 'id', '') is not null
      and nullif(btrim(classification.value ->> 'main_category'), '') is not null
),
chapters as (
    select
        c.chapter_id,
        c.video_id,
        c.start_segment_id
    from {{ ref('core_llm_chapters') }} c
),
classified as (
    select
        ch.chapter_id,
        rc.main_category,
        rc.subcategory,
        rc.reason
    from raw_classifications rc
    join chapters ch
      on ch.video_id = rc.video_id
     and ch.start_segment_id = rc.start_segment_id
),
with_categories as (
    select
        classified.chapter_id,
        categories.category_id,
        classified.subcategory,
        classified.reason
    from classified
    join {{ ref('core_llm_categories') }} categories
      on categories.main_category = classified.main_category
),
with_subcategories as (
    select
        wc.chapter_id,
        wc.category_id,
        subcategories.subcategory_id,
        wc.reason
    from with_categories wc
    left join {{ ref('core_llm_subcategories') }} subcategories
      on subcategories.category_id = wc.category_id
     and subcategories.subcategory = wc.subcategory
),
distinct_classifications as (
    select distinct
        chapter_id,
        category_id,
        subcategory_id,
        reason
    from with_subcategories
),
existing as (
    {% if is_incremental() %}
    select classification_id, chapter_id, category_id, subcategory_id
    from {{ this }}
    {% else %}
    select null::int as classification_id, null::int as chapter_id, null::int as category_id, null::int as subcategory_id
    where false
    {% endif %}
),
new_classifications as (
    select
        dc.chapter_id,
        dc.category_id,
        dc.subcategory_id,
        dc.reason
    from distinct_classifications dc
    left join existing e
      on e.chapter_id = dc.chapter_id
     and e.category_id = dc.category_id
     and (
         (e.subcategory_id is null and dc.subcategory_id is null)
         or e.subcategory_id = dc.subcategory_id
     )
    where e.classification_id is null
),
numbered_new as (
    select
        nc.chapter_id,
        nc.category_id,
        nc.subcategory_id,
        nc.reason,
        row_number() over (
            order by nc.chapter_id, nc.category_id, coalesce(nc.subcategory_id, 0)
        ) as new_rank
    from new_classifications nc
),
max_existing as (
    select coalesce(max(classification_id), 0) as max_id from existing
)

select
    max_existing.max_id + numbered_new.new_rank as classification_id,
    numbered_new.chapter_id,
    numbered_new.category_id,
    numbered_new.subcategory_id,
    numbered_new.reason
from numbered_new
cross join max_existing
