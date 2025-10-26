{{ config(
    unique_key='subcategory_id',
) }}

SELECT
    subcategory_id,
    subcategory
FROM {{ ref('core_subcategories') }}
