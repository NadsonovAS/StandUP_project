{{ config(
    materialized='table',
    indexes=[
      {'columns': ['channel_id'], 'unique': True},
      {'columns': ['channel_name']}
    ],
    post_hook=[
      "ALTER TABLE {{ this }} ADD PRIMARY KEY (channel_id)",
      "ANALYZE {{ this }}"
    ]
) }}

SELECT
    channel_id,
    channel_name
FROM {{ ref('core_channels') }}
