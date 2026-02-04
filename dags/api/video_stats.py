import json
import requests
from datetime import datetime
from airflow.decorators import task
from airflow.models import Variable


def get_api_key():
    return Variable.get("API_KEY")


def get_channel_handle():
    return Variable.get("CHANNEL_HANDLE")


@task
def get_playlist_id():
    api_key = get_api_key()
    channel_handle = get_channel_handle()
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"
    response = requests.get(url, headers={"Accept": "application/json"})
    response.raise_for_status()
    data = response.json()
    channel_items = data["items"][0]
    channel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
    return channel_playlist_id


@task
def get_playlist_videos(playlist_id):
    print("Getting Playlist Videos")
    api_key = get_api_key()
    video_ids = []
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&playlistId={playlist_id}&key={api_key}"
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


def batch_list(video_id_lst, batch_size=50):
    for i in range(0, len(video_id_lst), batch_size):
        yield video_id_lst[i : i + batch_size]


@task
def extract_video_stats(video_id_lst):
    api_key = get_api_key()
    video_stats = []
    for batch in batch_list(video_id_lst):
        video_ids = ",".join(batch)
        # base_url = f"https://youtube.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_ids}&key={api_key}"
        base_url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids}&key={api_key}"
        response = requests.get(base_url, headers={"Accept": "application/json"})
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            statistics = item.get("statistics", {})
            contentDetails = item["contentDetails"]
            video_data = {
                "video_id": item.get("id"),
                "title": snippet.get("title"),
                "publishedAt": snippet.get("publishedAt"),
                "duration": contentDetails.get("duration"),
                "viewCount": statistics.get("viewCount", None),
                "likeCount": statistics.get("likeCount", None),
                "commentCount": statistics.get("commentCount", None),
            }
            video_stats.append(video_data)

    return video_stats


@task
def save_to_json(extracted_data):
    file_path = f"./data/video_stats_{datetime.now().strftime('%Y-%m-%d')}.json"
    with open(file_path, "w") as f:
        json.dump(extracted_data, f, indent=4, ensure_ascii=False)
    print(f"Data saved to {file_path}")


if __name__ == "__main__":
    channel_playlist_id = get_playlist_id()
    if channel_playlist_id:
        video_ids = get_playlist_videos(channel_playlist_id)
    if video_ids:
        video_stats = extract_video_stats(video_ids)
        if video_stats:
            save_to_json(video_stats)
