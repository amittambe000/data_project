import json
import requests
from dotenv import load_dotenv
import os
from datetime import datetime

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


def batch_list(video_id_lst, batch_size=50):
    for i in range(0, len(video_id_lst), batch_size):
        yield video_id_lst[i : i + batch_size]


def extract_video_stats(video_id_lst):
    try:
        video_stats = []
        for batch in batch_list(video_id_lst):
            video_ids = ",".join(batch)
            base_url = f"https://youtube.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_ids}&key={API_KEY}"
            response = requests.get(base_url, headers={"Accept": "application/json"})
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                snippet = item.get("snippet", {})
                statistics = item.get("statistics", {})
                video_stats.append(
                    {
                        "video_id": item.get("id"),
                        "title": snippet.get("title"),
                        "description": snippet.get("description"),
                        "published_at": snippet.get("publishedAt"),
                        "channel_id": snippet.get("channelId"),
                        "channel_title": snippet.get("channelTitle"),
                        "view_count": statistics.get("viewCount"),
                        "like_count": statistics.get("likeCount"),
                        "comment_count": statistics.get("commentCount"),
                    }
                )
        return video_stats
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: {e}")
        return None


def save_to_json(extracted_data):
    try:
        file_path = (
            f"./data/video_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(file_path, "w") as f:
            json.dump(extracted_data, f, indent=4, ensure_ascii=False)
        print(f"Data saved to {file_path}")
    except Exception as e:
        print(f"Error: {e}")
        return None


if __name__ == "__main__":
    channel_playlist_id = get_playlist_id()
    if channel_playlist_id:
        video_ids = get_playlist_videos(channel_playlist_id)
    if video_ids:
        video_stats = extract_video_stats(video_ids)
        if video_stats:
            save_to_json(video_stats)
