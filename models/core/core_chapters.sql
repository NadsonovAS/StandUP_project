{{ config (
    materialized='incremental',
) }}

with stg_classifications as (
	select
		video_id,
		start_segment_id,
		main_category,
		subcategory
	from {{ ref("stg_classifications") }} stg_cl
	where is_valid is TRUE)
select
	stg_ch.video_id,
	stg_ch.start_segment_id,
	stg_ch.end_segment_id,
	sub.subcategory_id
from {{ ref("stg_chapters") }} stg_ch
join stg_classifications stg_cl on
	stg_ch.video_id = stg_cl.video_id
	and stg_ch.start_segment_id = stg_cl.start_segment_id
join {{source('standup_core', 'core_subcategories')}} sub on
	sub.subcategory = stg_cl.subcategory
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.video_id = stg_ch.video_id
)
{% endif %}