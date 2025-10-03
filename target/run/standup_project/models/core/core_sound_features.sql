
      insert into "standup_project"."standup_core"."sound_features" ("video_id", "time_offset", "score")
    (
        select "video_id", "time_offset", "score"
        from "sound_features__dbt_tmp233815752838"
    )


  