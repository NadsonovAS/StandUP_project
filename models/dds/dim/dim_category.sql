{{ config(
    materialized='table',
    indexes=[
      {'columns': ['category_id'], 'unique': True},
      {'columns': ['main_category']}
    ],
    post_hook=[
      "ALTER TABLE {{ this }} ADD PRIMARY KEY (category_id)",
      "ANALYZE {{ this }}"
    ]
) }}

SELECT
    category_id,
    main_category
FROM {{ source('standup_core', 'categories') }}