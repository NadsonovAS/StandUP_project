{{ config(
    unique_key='video_id',
) }}

SELECT
    video_id,
    video_title
FROM {{ ref('core_videos') }}
