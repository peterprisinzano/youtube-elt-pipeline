from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, gold_table
from dataquality.soda import youtube_elt_dq

# Define the local timezone
local_tz = pendulum.timezone("America/New_York")

# Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "peterprisinzano@gmail.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2030, 12, 31, tzinfo=local_tz)
}

with DAG(
    dag_id='extract_yt_api_data',
    default_args=default_args,
    description='DAG to extract data from Youtube API and collect raw data in JSON format',
    schedule='0 14 * * *',
    catchup=False
) as dag:
    
    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_json_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_json_data)

    # Define dependencies
    playlist_id >> video_ids >> extracted_json_data >> save_to_json_task


with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='DAG to process JSON file and insert data into staging and gold layers of the data warehouse',
    schedule='0 15 * * *',
    catchup=False
) as dag:
    
    # Define tasks
    update_staging = staging_table()
    update_gold = gold_table()

    # Define dependencies
    update_staging >> update_gold

with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='DAG to run DQ tests on staging and gold layers of the data warehouse',
    schedule='0 16 * * *',
    catchup=False
) as dag:
    
    # Define tasks
    soda_validate_staging = youtube_elt_dq(schema='staging')
    soda_validate_gold = youtube_elt_dq(schema='gold')

    # Define dependencies
    soda_validate_staging >> soda_validate_gold
    