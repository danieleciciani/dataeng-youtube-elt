from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

#Define the local timezone
local_tz = pendulum.timezone("Europe/Rome")

#default Dag args
default_args = {
    'owner': 'dciciani',
    'depends_on_part' : False,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'email' : 'danix.c96@gmail.com',
    #'retry' : 1,
    #'retry_delay': timedelta(minutes=5),
    'max_active_runs' : 1,
    'dagrun_timeout' :timedelta(hours=1),
    'start_date' : datetime(2026, 1, 1, tzinfo=local_tz)
    #'end_date' : datetime(2026, 2, 31, tzifno=local_t)
}

with DAG (
    dag_id = 'produce_json',
    default_args = default_args,
    description = 'Dag to produce Json file with Raw data',
    schedule='0 14 * * *',
    catchup = False,
) as dag:

    # Define tasks
    playlist_id =   get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    # Define dependencies

    playlist_id >> video_ids >> extracted_data >> save_to_json_task
