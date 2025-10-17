{{ config(
    materialized='incremental',
    unique_key='channel_id'
) }}

select distinct 
    channel_id, 
    channel_name,
    current_timestamp as created_at
from {{ ref("stg_videos_base") }} stg_v
where is_valid is true and
{% if is_incremental() %}
    not exists (
    select 1
    from {{ this }} existing
    where existing.channel_id = stg_v.channel_id
)
{% endif %}