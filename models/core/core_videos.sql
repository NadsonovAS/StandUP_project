{{ config(
    materialized='incremental',
    alias='videos',
    incremental_strategy='merge',
    unique_key='yt_video_id',
    on_schema_change='sync_all_columns',
    merge_update_columns=[
        'channel_id',
        'playlist_id',
        'video_title',
        'video_url',
        'duration',
        'like_count',
        'view_count',
        'comment_count',
        'upload_date',
        'updated_at'
    ]
) }}

with source_channels as (
    select
        pr.video_id as yt_video_id,
        ch.channel_id,
        pl.playlist_id,
        pr.video_title,
        pr.video_url,
        (pr.video_meta_json ->> 'duration')::int2 as duration,
        (pr.video_meta_json ->> 'like_count')::int as like_count,
        (pr.video_meta_json ->> 'view_count')::int as view_count,
        (pr.video_meta_json ->> 'comment_count')::int as comment_count,
        (pr.video_meta_json ->> 'upload_date')::date as upload_date,
        pr.updated_at
    from {{ source('standup_raw', 'process_video') }} pr
    join {{ ref('core_channels') }} ch on ch.yt_channel_id = pr.channel_id
    join {{ ref('core_playlists') }} pl on pl.yt_playlist_id = pr.playlist_id
    where pr.process_status = 'finished'
)
{% if is_incremental() %},
existing as (
    select
        yt_video_id,
        video_id,
        updated_at,
        created_at
    from {{ this }}
)
{% endif %}

select
    {% if is_incremental() %}
    coalesce(existing.video_id, nextval('standup_core.videos_video_id_seq')) as video_id,
    {% else %}
    nextval('standup_core.videos_video_id_seq') as video_id,
    {% endif %}
    sc.yt_video_id,
    sc.channel_id,
    sc.playlist_id,
    sc.video_title,
    sc.video_url,
    sc.duration,
    sc.like_count,
    sc.view_count,
    sc.comment_count,
    sc.upload_date,
    {% if is_incremental() %}
    coalesce(existing.created_at, current_timestamp) as created_at,
    {% else %}
    current_timestamp as created_at,
    {% endif %}
    sc.updated_at
from source_channels sc
{% if is_incremental() %}
left join existing on existing.yt_video_id = sc.yt_video_id
where existing.video_id is null
   or existing.updated_at is null
   or sc.updated_at is null
   or sc.updated_at > existing.updated_at
{% endif %}
