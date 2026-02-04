from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)


def load_data():
    try:
        file_path = f"./data/video_stats_{datetime.now().strftime('%Y-%m-%d')}.json"
        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)
            return data
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error: {e}")
        raise
