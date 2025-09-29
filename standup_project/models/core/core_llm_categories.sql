{{ config(
    materialized='incremental',
    alias='llm_categories',
    on_schema_change='ignore'
) }}

with raw_categories as (
    select distinct
        nullif(classification.value ->> 'main_category', '') as main_category
    from {{ ref('stg_process_video') }} sp
    join {{ ref('core_videos') }} v on v.video_id = sp.video_id
    cross join lateral jsonb_array_elements(
        coalesce(sp.llm_classifier_json -> 'classifications', '[]'::jsonb)
    ) as classification(value)
),
valid_categories as (
    select main_category
    from raw_categories
    where main_category is not null
),
existing as (
    {% if is_incremental() %}
    select category_id, main_category
    from {{ this }}
    {% else %}
    select null::int as category_id, null::text as main_category
    where false
    {% endif %}
),
new_categories as (
    select vc.main_category
    from valid_categories vc
    left join existing e on e.main_category = vc.main_category
    where e.main_category is null
),
numbered_new as (
    select
        nc.main_category,
        row_number() over (order by nc.main_category) as new_rank
    from new_categories nc
),
max_existing as (
    select coalesce(max(category_id), 0) as max_id from existing
)

select
    max_existing.max_id + numbered_new.new_rank as category_id,
    numbered_new.main_category
from numbered_new
cross join max_existing
