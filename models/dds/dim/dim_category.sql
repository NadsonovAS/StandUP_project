{{ config(
    materialized='table',
    indexes=[
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
FROM {{ ref('core_categories') }}
