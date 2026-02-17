import os
from googleapiclient.discovery import build
from dotenv import load_dotenv
load_dotenv()

class YouTubeClient:



    def __init__(self):
        self.api_key = os.getenv("YOUTUBE_API_KEY")
        self.channel_id = os.getenv("YOUTUBE_CHANNEL_ID")

        if not self.api_key:
            raise ValueError("YOUTUBE_API_KEY not configured")

        if not self.channel_id:
            raise ValueError("YOUTUBE_CHANNEL_ID not configured")

        self.youtube = build("youtube", "v3", developerKey=self.api_key)




    def get_recent_video_ids(self, max_results=5):
        response = self.youtube.search().list(
            part="snippet",
            channelId=self.channel_id,
            order="date",
            maxResults=max_results,
            type="video"
        ).execute()

        return [
            item["id"]["videoId"]
            for item in response["items"]
        ]




    def get_videos_details(self, video_ids):
        if not video_ids:
            return []

        response = self.youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(video_ids)
        ).execute()

        return response["items"]



    def get_channel_stats(self):
        response = self.youtube.channels().list(
            part="statistics",
            id=self.channel_id
        ).execute()

        if not response["items"]:
            return None

        stats = response["items"][0]["statistics"]

        return {
            "channel_id": self.channel_id,
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "view_count": int(stats.get("viewCount", 0)),
            "video_count": int(stats.get("videoCount", 0))
        }



    def get_all_video_ids(self):
        channel_response = self.youtube.channels().list(
            part="contentDetails",
            id=self.channel_id
        ).execute()

        if not channel_response["items"]:
            raise ValueError("Channel not found")

        uploads_playlist_id = (
            channel_response["items"][0]
            ["contentDetails"]
            ["relatedPlaylists"]
            ["uploads"]
        )

        video_ids = []
        next_page_token = None

        while True:
            playlist_response = self.youtube.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            video_ids.extend([
                item["contentDetails"]["videoId"]
                for item in playlist_response["items"]
            ])

            next_page_token = playlist_response.get("nextPageToken")

            if not next_page_token:
                break

        return video_ids

    
    
    def get_channel_snippet(self):
        response = self.youtube.channels().list(
            part="snippet",
            id=self.channel_id
        ).execute()

        if not response["items"]:
            return None

        return response["items"][0]["snippet"]


    def get_all_playlists(self):
        playlists = []
        next_page_token = None

        while True:
            response = self.youtube.playlists().list(
                part="snippet,contentDetails,status",
                channelId=self.channel_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            playlists.extend(response["items"])

            next_page_token = response.get("nextPageToken")

            if not next_page_token:
                break

        return playlists



    def get_uploads_playlist_id(self):
        response = self.youtube.channels().list(
            part="contentDetails",
            id=self.channel_id
        ).execute()

        if not response["items"]:
            raise ValueError("Channel not found")

        return (
            response["items"][0]
            ["contentDetails"]
            ["relatedPlaylists"]
            ["uploads"]
        )


    def get_playlist_items(self, playlist_id):
        items = []
        next_page_token = None

        while True:
            response = self.youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            items.extend(response["items"])

            next_page_token = response.get("nextPageToken")

            if not next_page_token:
                break

        return items
