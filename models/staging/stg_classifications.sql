{{ config(
    materialized='view',
) }}

WITH process_video AS (
    SELECT
        video_id,
        llm_classifier_json
    FROM {{ source('standup_raw', 'process_video') }}
    WHERE process_status = 'finished' AND llm_classifier_json IS NOT NULL
),

parsed_chapters AS (
    SELECT
        video_id::TEXT AS video_id,
        cl.id AS start_segment_id,
        cl.reason,
        cl.subcategory,
        cl.main_category
    FROM process_video
    CROSS JOIN
        LATERAL jsonb_to_recordset(llm_classifier_json -> 'classifications')
            AS cl (id INT, reason TEXT, subcategory TEXT, main_category TEXT)
)

SELECT
    video_id,
    start_segment_id,
    trim(reason) AS reason,
    trim(subcategory) AS subcategory,
    trim(main_category) AS main_category,

    -- Validate
    NOT coalesce(coalesce(reason, subcategory, main_category) IS NULL, FALSE) AS is_valid

FROM parsed_chapters
