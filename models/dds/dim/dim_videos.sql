{{ config(
    materialized='table',
    indexes=[
      {'columns': ['video_title']}
    ],
    post_hook=[
      "ALTER TABLE {{ this }} ADD PRIMARY KEY (video_id)",
      "ANALYZE {{ this }}"
    ]
) }}

SELECT
    video_id,
    video_title
FROM {{ ref('core_videos') }}
