import json
import requests
from dotenv import load_dotenv
import os

load_dotenv("./.env")
API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")
URL = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"


def get_playlist_id():
    try:
        response = requests.get(URL, headers={"Accept": "application/json"})
        response.raise_for_status()
        data = response.json()
        channel_items = data["items"][0]
        channel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"][
            "uploads"
        ]
        return channel_playlist_id
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: {e}")
        return None
    except KeyError as e:
        print(f"Error: {e}")
        return None
    except IndexError as e:
        print(f"Error: {e}")


def get_playlist_videos(playlist_id):
    print("Gettig Playlist Videos")
    video_ids = []
    try:
        base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&playlistId={playlist_id}&key={API_KEY}"
        page_token = None
        while True:
            url = base_url
            if page_token:
                url += f"&pageToken={page_token}"

            response = requests.get(url, headers={"Accept": "application/json"})
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                video_id = item.get("contentDetails").get("videoId")
                video_ids.append(video_id)
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        return video_ids
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: {e}")
        return None


if __name__ == "__main__":
    channel_playlist_id = get_playlist_id()
    if channel_playlist_id:
        video_ids = get_playlist_videos(channel_playlist_id)
        if video_ids:
            print(video_ids)
