{{ config(
    materialized='incremental',
    alias='channels',
    on_schema_change='ignore'
) }}

with source_channels as (
    select distinct
        nullif(sp.channel_id, '') as channel_id,
        coalesce(nullif(sp.channel_name, ''), 'Unknown Channel') as channel_name
    from {{ ref('stg_process_video') }} sp
    where sp.channel_id is not null and sp.channel_id <> ''
)

select
    sc.channel_id,
    sc.channel_name,
    current_timestamp as created_at
from source_channels sc
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.channel_id = sc.channel_id
)
{% endif %}
