from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import (
    get_playlist_id,
    get_playlist_videos,
    extract_video_stats,
    save_to_json,
)
from dataquality.soda import yt_elt_data_quality
from datawarehouse.dwh import staging_table, core_table

local_tz = pendulum.timezone("Asia/Kolkata")
staging_schema = "staging"
core_schema = "core"


default_args = {
    "owner": "amit",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["test@gmail.com"],
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=60),
    "start_date": datetime(2026, 2, 3, tzinfo=local_tz),
}

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="Dag to product JSON",
    schedule_interval=None,
    catchup=False,
) as dag:
    playlist_id = get_playlist_id()
    video_ids = get_playlist_videos(playlist_id)
    video_stats = extract_video_stats(video_ids)
    save_to_json_task = save_to_json(video_stats)

    playlist_id >> video_ids >> video_stats >> save_to_json_task
# DAG 2: update_db
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    catchup=False,
    schedule=None,
) as dag_update:

    # Define tasks
    update_staging = staging_table()  # pyright: ignore[reportUndefinedVariable]
    update_core = core_table()
    # Define dependencies
    update_staging >> update_core

# DAG 3: data_quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both layers in the database",
    catchup=False,
    schedule=None,
) as dag_quality:

    # Define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    # Define dependencies
    soda_validate_staging >> soda_validate_core
