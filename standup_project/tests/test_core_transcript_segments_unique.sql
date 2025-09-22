-- Ensure transcript segments are unique per video
select
    video_id,
    segment_id
from {{ ref('core_transcript_segments') }}
group by 1, 2
having count(*) > 1
