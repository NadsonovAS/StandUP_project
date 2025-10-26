{{ config(
    unique_key='playlist_id',
) }}

SELECT
    playlist_id,
    playlist_title
FROM {{ ref('core_playlists') }}
