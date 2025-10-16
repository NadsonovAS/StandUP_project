{{ config(
    materialized='table',
    indexes=[
      {'columns': ['playlist_id'], 'unique': True},
      {'columns': ['playlist_title']}
    ],
    post_hook=[
      "ALTER TABLE {{ this }} ADD PRIMARY KEY (playlist_id)",
      "ANALYZE {{ this }}"
    ]
) }}

SELECT
    playlist_id,
    yt_playlist_id,
    playlist_title
FROM {{ ref('core_playlists') }}