{{ config (
    materialized='incremental',
) }}

select
    stg_tr.video_id, 
    stg_tr.segment_id,
    stg_tr.segment_text,
    stg_tr.start_s,
    stg_tr.end_s
from {{ ref("stg_transcripts") }} stg_tr
where stg_tr.is_valid is TRUE and
{% if is_incremental() %}
    not exists (
    select 1
    from {{ this }} existing
    where existing.video_id = stg_tr.video_id
)
{% endif %}
