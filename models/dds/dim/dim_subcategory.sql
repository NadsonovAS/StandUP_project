{{ config(
    materialized='table',
    indexes=[
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
FROM {{ ref('core_subcategories') }}
