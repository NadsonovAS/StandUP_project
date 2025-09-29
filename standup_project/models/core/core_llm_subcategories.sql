{{ config(
    materialized='incremental',
    alias='llm_subcategories',
    on_schema_change='ignore'
) }}

with source_subcategories as (
    select distinct
        nullif(classification.value ->> 'main_category', '') as main_category,
        nullif(classification.value ->> 'subcategory', '') as subcategory
    from {{ ref('stg_process_video') }} sp
    join {{ ref('core_videos') }} v on v.video_id = sp.video_id
    cross join lateral jsonb_array_elements(
        coalesce(sp.llm_classifier_json -> 'classifications', '[]'::jsonb)
    ) as classification(value)
    where nullif(classification.value ->> 'main_category', '') is not null
      and nullif(classification.value ->> 'subcategory', '') is not null
),
linked_categories as (
    select distinct
        cat.category_id,
        ss.subcategory
    from source_subcategories ss
    join {{ ref('core_llm_categories') }} cat
      on cat.main_category = ss.main_category
),
existing as (
    {% if is_incremental() %}
    select subcategory_id, category_id, subcategory
    from {{ this }}
    {% else %}
    select null::int as subcategory_id, null::int as category_id, null::text as subcategory
    where false
    {% endif %}
),
new_subcategories as (
    select lc.category_id, lc.subcategory
    from linked_categories lc
    left join existing e
      on e.category_id = lc.category_id
     and e.subcategory = lc.subcategory
    where e.subcategory_id is null
),
numbered_new as (
    select
        ns.category_id,
        ns.subcategory,
        row_number() over (order by ns.category_id, ns.subcategory) as new_rank
    from new_subcategories ns
),
max_existing as (
    select coalesce(max(subcategory_id), 0) as max_id from existing
)

select
    max_existing.max_id + numbered_new.new_rank as subcategory_id,
    numbered_new.category_id,
    numbered_new.subcategory
from numbered_new
cross join max_existing
