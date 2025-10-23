{{ config (
    materialized='incremental',
) }}

with stg_classifications as (
    select
        video_id,
        start_segment_id,
        main_category,
        subcategory
    from {{ ref("stg_classifications") }}
    where is_valid is TRUE
)

select
    stg_ch.video_id,
    stg_ch.start_segment_id,
    stg_ch.end_segment_id,
    sub.subcategory_id
from {{ ref("stg_chapters") }} as stg_ch
inner join stg_classifications as stg_cl
    on
        stg_ch.video_id = stg_cl.video_id
        and stg_ch.start_segment_id = stg_cl.start_segment_id
inner join {{ ref('core_subcategories') }} as sub
    on
        stg_cl.subcategory = sub.subcategory
{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as existing
        where existing.video_id = stg_ch.video_id
    )
{% endif %}
