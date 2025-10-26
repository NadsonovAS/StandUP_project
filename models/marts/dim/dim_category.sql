{{ config(
    unique_key='category_id',
) }}

SELECT
    category_id,
    main_category
FROM {{ ref('core_categories') }}
