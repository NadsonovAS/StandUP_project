{{ config (
    unique_key=['video_id', 'segment_id']
) }}

select
    stg_tr.video_id,
    stg_tr.segment_id,
    stg_tr.segment_text,
    stg_tr.start_s,
    stg_tr.end_s
from {{ ref("stg_transcripts") }} as stg_tr
where
    stg_tr.is_valid is TRUE
    {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where existing.video_id = stg_tr.video_id
        )
    {% endif %}
