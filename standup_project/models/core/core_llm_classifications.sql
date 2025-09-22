{{ config(materialized='table') }}

with base as (
    select
        sp.video_id,
        concat_ws('_', sp.video_id, classification.value ->> 'id') as chapter_id,
        nullif(btrim(classification.value ->> 'main_category'), '') as main_category,
        nullif(btrim(classification.value ->> 'subcategory'), '') as subcategory,
        classification.value ->> 'reason' as reason
    from {{ ref('stg_process_video') }} sp
    cross join lateral jsonb_array_elements(coalesce(sp.llm_classifier_json -> 'classifications', '[]'::jsonb)) as classification(value)
    where nullif(classification.value ->> 'id', '') is not null
)
,
categories as (
    select
        main_category,
        row_number() over (order by main_category) as category_id
    from (
        select distinct main_category
        from base
        where nullif(main_category, '') is not null
    ) distinct_categories
)

select
    base.video_id,
    base.chapter_id,
    categories.category_id,
    base.main_category,
    base.subcategory,
    base.reason
from base
join categories using (main_category)
