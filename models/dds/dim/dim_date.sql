{{ config(
    materialized='table',
    indexes=[
      {'columns': ['date'], 'unique': True},
    ],
    post_hook=[
      "ALTER TABLE {{ this }} ADD PRIMARY KEY (date_id)",
      "ANALYZE {{ this }}"
    ]
) }}

SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_id,
    d::DATE AS date,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(MONTH FROM d)::INT AS month,
    EXTRACT(WEEK FROM d)::INT AS week_of_year,
    EXTRACT(ISODOW FROM d)::INT AS day_of_week,
    TO_CHAR(d, 'Month') AS month_name,
    TO_CHAR(d, 'Day') AS day_name,
    (EXTRACT(ISODOW FROM d) IN (6, 7)) AS is_weekend
FROM
    GENERATE_SERIES('2005-02-14'::DATE, '2030-12-31'::DATE, INTERVAL '1 day')
        AS d
