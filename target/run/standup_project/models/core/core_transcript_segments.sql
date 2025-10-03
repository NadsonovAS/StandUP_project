
      insert into "standup_project"."standup_core"."transcript_segments" ("video_id", "segment_id", "start_s", "end_s", "text")
    (
        select "video_id", "segment_id", "start_s", "end_s", "text"
        from "transcript_segments__dbt_tmp233815753998"
    )


  