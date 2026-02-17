from scripts.pipeline import (
    update_latest_videos_current,
    update_channels_snapshot,
    update_channels_static,
    update_videos_static,
    update_playlists_manual_static,
    update_playlist_items_manual_static,
    update_playlist_items_snapshot,
    update_videos_snapshot
)

# 🔹 DAILY
def daily(request=None):
    update_channels_snapshot()
    update_latest_videos_current()
    return "Daily pipeline executed successfully.", 200


# 🔹 WEEKLY
def weekly(request=None):
    # 1️⃣ Static primero
    update_channels_static()
    update_videos_static()
    df_playlists_manual_static = update_playlists_manual_static()
    df_playlist_items_manual_static = update_playlist_items_manual_static(df_playlists_manual_static)
    
     # 2️⃣ Snapshots después
    update_playlist_items_snapshot(df_playlist_items_manual_static)
    update_videos_snapshot()

    return "Weekly pipeline executed successfully.", 200




    