{{ config(
    materialized='incremental',
    alias='channels'
) }}

with stg_video_base as (
    select distinct yt_channel_id, channel_name
    from {{ ref("stg_videos_base") }}
    where is_valid is true
)

select
    nextval('standup_core.channels_channel_id_seq') as channel_id,
    yt_channel_id,
    channel_name,
    current_timestamp as created_at
from stg_video_base stg_v
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.yt_channel_id = stg_v.yt_channel_id
)
{% endif %}