{{ config(
    materialized='incremental',
    unique_key='playlist_id'
) }}

select distinct
    playlist_id,
    playlist_title,
    current_timestamp as created_at
from {{ ref("stg_videos_base") }} as stg_v
where
    is_valid is true and
    {% if is_incremental() %}
        not exists (
            select 1
            from {{ this }} as existing
            where existing.playlist_id = stg_v.playlist_id
        )
    {% endif %}
