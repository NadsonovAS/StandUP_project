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
    TO_CHAR(d, 'YYYYMMDD')::INT          AS date_id,
    d::date                              AS date,
    EXTRACT(YEAR FROM d)::int            AS year,
    EXTRACT(QUARTER FROM d)::int         AS quarter,
    EXTRACT(MONTH FROM d)::int           AS month,
    TO_CHAR(d, 'Month')                  AS month_name,
    EXTRACT(WEEK FROM d)::int            AS week_of_year,
    EXTRACT(ISODOW FROM d)::int          AS day_of_week,
    TO_CHAR(d, 'Day')                    AS day_name,
    (EXTRACT(ISODOW FROM d) IN (6,7))    AS is_weekend
FROM generate_series('2005-02-14'::date, '2030-12-31'::date, interval '1 day') d