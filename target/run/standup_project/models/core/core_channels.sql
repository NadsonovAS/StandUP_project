
      insert into "standup_project"."standup_core"."channels" ("channel_id", "yt_channel_id", "created_at", "channel_name")
    (
        select "channel_id", "yt_channel_id", "created_at", "channel_name"
        from "channels__dbt_tmp233815577521"
    )


  