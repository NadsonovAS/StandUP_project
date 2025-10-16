{{ config(
    materialized='view',
    alias='stg_classification'
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
        video_id::TEXT as yt_video_id,
        cl.id as start_segment_id,
        cl.reason as reason,
        cl.subcategory as subcategory,
        cl.main_category as main_category
    FROM process_video
    CROSS JOIN LATERAL jsonb_to_recordset(llm_classifier_json -> 'classifications')
        as cl(id int, reason text, subcategory text, main_category text)
)

SELECT 
    yt_video_id,
    start_segment_id,
    TRIM(reason) as reason,
    TRIM(subcategory) as subcategory,
    TRIM(main_category) as main_category,
    
    -- Validate
    CASE
        WHEN COALESCE(reason, subcategory, main_category) IS NULL THEN FALSE
        ELSE TRUE
    END as is_valid
    
FROM parsed_chapters