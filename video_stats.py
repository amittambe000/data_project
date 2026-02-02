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
        print(channel_playlist_id)
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
        return None


if __name__ == "__main__":
    get_playlist_id()
