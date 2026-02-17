from datetime import datetime, timezone, date
import pandas as pd
import isodate

from scripts.youtube_client import YouTubeClient
from scripts.bigquery_repository import BigQueryRepository
from datetime import datetime, timedelta, timezone



def iso_duration_to_seconds(duration):
    try:
        return int(isodate.parse_duration(duration).total_seconds())
    except Exception:
        return None


def get_current_week_monday():
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return monday


# DIARIO - CURRENT - devuelve los 5 videos recientes
def update_latest_videos_current():
    youtube_client = YouTubeClient()
    bq_repository = BigQueryRepository()

    # 1️⃣ Extract
    video_ids = youtube_client.get_recent_video_ids(max_results=5)
    videos = youtube_client.get_videos_details(video_ids)

    # 2️⃣ Transform
    rows = []

    for item in videos:
        snippet = item["snippet"]
        stats = item["statistics"]

        rows.append({
            "video_id": item["id"],
            "title": snippet["title"],
            "published_at": snippet["publishedAt"],
            "thumbnail_url": snippet["thumbnails"]["high"]["url"],
            "video_url": f"https://www.youtube.com/watch?v={item['id']}",
            "view_count": int(stats.get("viewCount", 0)),
            "like_count": int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0)),
            "extracted_at": datetime.now(timezone.utc)
        })

    df = pd.DataFrame(rows)

    df["published_at"] = pd.to_datetime(df["published_at"], utc=True)
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)

    # 3️⃣ Load
    bq_repository.load_dataframe(
        table_name="latest_videos_current",
        dataframe=df,
        write_disposition="WRITE_TRUNCATE"
    )


#DIARIO - SNAPSHOT - agrega una fila con las métricas globales del canal
def update_channels_snapshot():
    youtube_client = YouTubeClient()
    bq_repository = BigQueryRepository()

    # 1️⃣ Extract
    channel_stats = youtube_client.get_channel_stats()

    if not channel_stats:
        raise ValueError("Channel stats not found")

    snapshot_date = date.today()

    # 2️⃣ Transform
    row = {
        "snapshot_date": snapshot_date,
        "channel_id": channel_stats["channel_id"],
        "subscriber_count": channel_stats["subscriber_count"],
        "view_count": channel_stats["view_count"],
        "video_count": channel_stats["video_count"],
        "extracted_at": datetime.now(timezone.utc)
    }

    df = pd.DataFrame([row])

    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)

    # 3️⃣ Load
    bq_repository.delete_snapshot_by_date(
        table_name="channels_snapshot",
        snapshot_date=snapshot_date
    )

    bq_repository.load_dataframe(
        table_name="channels_snapshot",
        dataframe=df,
        write_disposition="WRITE_APPEND"
    )


#SEMANAL - STATIC - reescribe la metadata (imagen, descripción, etc) del canal
def update_channels_static():
    youtube_client = YouTubeClient()
    bq_repository = BigQueryRepository()

    # 1️⃣ Extract
    snippet = youtube_client.get_channel_snippet()

    if not snippet:
        raise ValueError("Channel not found")

    # 2️⃣ Transform
    row = {
        "channel_id": youtube_client.channel_id,
        "channel_title": snippet.get("title"),
        "description": snippet.get("description"),
        "country": snippet.get("country"),
        "published_at": snippet.get("publishedAt"),
        "thumbnail_url": snippet["thumbnails"]["high"]["url"]
            if "high" in snippet["thumbnails"] else None,
        "channel_url": f"https://www.youtube.com/channel/{youtube_client.channel_id}",
        "extracted_at": datetime.now(timezone.utc)
    }

    df = pd.DataFrame([row])

    df["published_at"] = pd.to_datetime(df["published_at"], utc=True)
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)

    # 3️⃣ Load
    bq_repository.load_dataframe(
        table_name="channels_static",
        dataframe=df,
        write_disposition="WRITE_TRUNCATE"
    )



#SEMANAL - STATIC - reescribe la tabla de videos 
def update_videos_static():
    youtube_client = YouTubeClient()
    bq_repository = BigQueryRepository()

    # 1️⃣ Extract
    video_ids = youtube_client.get_all_video_ids()

    if not video_ids:
        raise ValueError("No videos found")

    all_videos = []

    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i+50]
        videos = youtube_client.get_videos_details(batch_ids)
        all_videos.extend(videos)

    # 2️⃣ Transform
    rows = []

    for item in all_videos:
        snippet = item["snippet"]
        content = item.get("contentDetails", {})

        rows.append({
            "video_id": item["id"],
            "channel_id": snippet.get("channelId"),
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "category_id": snippet.get("categoryId"),
            "published_at": snippet.get("publishedAt"),
            "duration_seconds": iso_duration_to_seconds(content.get("duration")), 
            "thumbnail_url": snippet["thumbnails"]["high"]["url"]
                if "high" in snippet["thumbnails"] else None,
            "video_url": f"https://www.youtube.com/watch?v={item['id']}",
            "extracted_at": datetime.now(timezone.utc)
        })

    df = pd.DataFrame(rows)

    df["published_at"] = pd.to_datetime(df["published_at"], utc=True)
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)

    # 3️⃣ Load (reemplazo completo)
    bq_repository.load_dataframe(
        table_name="videos_static",
        dataframe=df,
        write_disposition="WRITE_TRUNCATE"
    )




#SEMANAL - STATIC - reescribe la tabla de playlist manuales 
def update_playlists_manual_static():
    youtube_client = YouTubeClient()
    bq_repository = BigQueryRepository()

    # 1️⃣ Extract
    playlists = youtube_client.get_all_playlists()
    uploads_playlist_id = youtube_client.get_uploads_playlist_id()

    # 2️⃣ Transform
    rows = []

    for item in playlists:
        if item["id"] == uploads_playlist_id:
            continue

        snippet = item["snippet"]
        content = item.get("contentDetails", {})
        status = item.get("status", {})

        rows.append({
            "playlist_id": item["id"],
            "channel_id": snippet.get("channelId"),
            "title": snippet.get("title"),
            "description": snippet.get("description", ""),
            "item_count": content.get("itemCount", 0),
            "privacy_status": status.get("privacyStatus"),
            "published_at": snippet.get("publishedAt"),
            "thumbnail_url": (
                snippet.get("thumbnails", {})
                       .get("high", {})
                       .get("url")
            ),
            "playlist_url": f"https://www.youtube.com/playlist?list={item['id']}",
            "extracted_at": datetime.now(timezone.utc)
        })

    df = pd.DataFrame(rows)

    df["published_at"] = pd.to_datetime(df["published_at"], utc=True)
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)

    # 3️⃣ Load
    bq_repository.load_dataframe(
        table_name="playlists_manual_static",
        dataframe=df,
        write_disposition="WRITE_TRUNCATE"
    )
    
    return df




#SEMANAL - STATIC - relación playlists manuales ↔ video (estado actual)
def update_playlist_items_manual_static(df_playlists_manual_static):

    # 1️⃣ Extract 2️⃣ Transform    
    rows = []

    youtube_client = YouTubeClient()
    bq_repository = BigQueryRepository()

    for _, playlist in df_playlists_manual_static.iterrows():

        playlist_id = playlist["playlist_id"]

        items = youtube_client.get_playlist_items(playlist_id)

        for item in items:
            rows.append({
                "playlist_id": playlist_id,
                "video_id": item["contentDetails"]["videoId"],
                "position": item["snippet"]["position"],
                "added_at": item["snippet"]["publishedAt"],
                "extracted_at": datetime.now(timezone.utc)
            })

    df = pd.DataFrame(rows)

    df["added_at"] = pd.to_datetime(df["added_at"], utc=True)
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)

    # 3️⃣ Load
    bq_repository.load_dataframe(
        table_name="playlist_items_manual_static",
        dataframe=df,
        write_disposition="WRITE_TRUNCATE"
    )

    return df





#SEMANAL - agrega columnas de la fotografía de playlist_items_manual_static
def update_playlist_items_snapshot(df_playlist_items):
    bq_repository = BigQueryRepository()

    snapshot_date = get_current_week_monday()

    df_snapshot = df_playlist_items.copy()
    df_snapshot["snapshot_date"] = snapshot_date
    df_snapshot["snapshot_date"] = pd.to_datetime(
        df_snapshot["snapshot_date"]
    ).dt.date

    df_snapshot = df_snapshot[
        [
            "snapshot_date",
            "playlist_id",
            "video_id",
            "position",
            "added_at",
            "extracted_at",
        ]
    ]

    bq_repository.delete_snapshot_by_date(
        table_name="playlist_items_snapshot",
        snapshot_date=snapshot_date
    )

    bq_repository.load_dataframe(
        table_name="playlist_items_snapshot",
        dataframe=df_snapshot,
        write_disposition="WRITE_APPEND"
    )






#SEMANAL - SNAPHOST - Agrega filas de acuerdo a cada video del canal con sus métricas
def update_videos_snapshot():
    youtube_client = YouTubeClient()
    bq_repository = BigQueryRepository()

    # 📅 snapshot_date = lunes ISO
    snapshot_date = get_current_week_monday()

    # 1️⃣ Extract
    video_ids = youtube_client.get_all_video_ids()

    if not video_ids:
        raise ValueError("No videos found")

    # Obtener detalles en bloques de 50 (YouTube límite)
    all_videos = []

    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i+50]
        videos = youtube_client.get_videos_details(batch_ids)
        all_videos.extend(videos)

    # 2️⃣ Transform
    rows = []

    for item in all_videos:
        snippet = item["snippet"]
        stats = item["statistics"]

        rows.append({
            "snapshot_date": snapshot_date,
            "video_id": item["id"],
            "channel_id": snippet["channelId"],
            "published_at": snippet["publishedAt"],
            "duration_seconds": iso_duration_to_seconds(item.get("contentDetails", {}).get("duration")),
            "view_count": int(stats.get("viewCount", 0)),
            "like_count": int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0)),
            "extracted_at": datetime.now(timezone.utc)
        })

    df = pd.DataFrame(rows)

    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    df["published_at"] = pd.to_datetime(df["published_at"], utc=True)
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)

    # 3️⃣ Load (idempotente)
    bq_repository.delete_snapshot_by_date(
        table_name="videos_snapshot",
        snapshot_date=snapshot_date
    )

    bq_repository.load_dataframe(
        table_name="videos_snapshot",
        dataframe=df,
        write_disposition="WRITE_APPEND"
    )



