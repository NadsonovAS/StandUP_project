
      insert into "standup_project"."standup_core"."videos" ("video_id", "playlist_id", "channel_id", "duration", "like_count", "view_count", "comment_count", "upload_date", "yt_video_id", "video_title", "video_url")
    (
        select "video_id", "playlist_id", "channel_id", "duration", "like_count", "view_count", "comment_count", "upload_date", "yt_video_id", "video_title", "video_url"
        from "videos__dbt_tmp233815701436"
    )


  