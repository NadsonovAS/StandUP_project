{{ config(
    materialized='incremental',
    unique_key='channel_id'
) }}

select distinct
    channel_id,
    channel_name,
    current_timestamp as created_at
from {{ ref("stg_videos_base") }} as stg_v
where
    is_valid is true
    {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where existing.channel_id = stg_v.channel_id
        )
    {% endif %}
