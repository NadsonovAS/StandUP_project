{{ config(
    unique_key='channel_id',
) }}

SELECT
    channel_id,
    channel_name
FROM {{ ref('core_channels') }}
