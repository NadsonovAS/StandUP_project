
      insert into "standup_project"."standup_core"."playlists" ("playlist_id", "created_at", "yt_playlist_id", "playlist_title")
    (
        select "playlist_id", "created_at", "yt_playlist_id", "playlist_title"
        from "playlists__dbt_tmp233815567209"
    )


  