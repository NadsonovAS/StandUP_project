{{ config(
    materialized='table',
    indexes=[
      {'columns': ['subcategory_id'], 'unique': True},
      {'columns': ['subcategory']}
    ],
    post_hook=[
      "ALTER TABLE {{ this }} ADD PRIMARY KEY (subcategory_id)",
      "ANALYZE {{ this }}"
    ]
) }}

SELECT
    subcategory_id,
    subcategory
FROM {{ source('standup_core', 'subcategories') }}