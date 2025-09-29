{{ config(
    materialized='incremental',
    alias='playlists',
    on_schema_change='ignore'
) }}

with source_playlists as (
    select distinct
        nullif(sp.playlist_id, '') as playlist_id,
        nullif(sp.playlist_title, '') as playlist_title
    from {{ ref('stg_process_video') }} sp
    where sp.playlist_id is not null and sp.playlist_id <> ''
)

select
    sp.playlist_id,
    sp.playlist_title,
    current_timestamp as created_at
from source_playlists sp
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.playlist_id = sp.playlist_id
)
{% endif %}
