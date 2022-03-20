"""
Sparkify Airflow DAG

1. Load data from S3 into Redshift
2. Create and load fact table.
3. Create and load dimention tables.
4. Run data quality checks.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from plugins.helpers import SqlQueries

default_args = {
    "owner": "udacity",
    "start_date": datetime(2019, 1, 12),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}

dag = DAG(
    "udac_example_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
    catchup=False,
)

start_operator = DummyOperator(
    task_id="Begin_execution",
    dag=dag,
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log-data",
    file_format="JSON 's3://udacity-dend/log_json_path.json'",
    create_table_sql=SqlQueries.create_table_staging_events,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    file_format="JSON 'auto'",
    create_table_sql=SqlQueries.create_table_staging_songs,
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    create_table_sql=SqlQueries.create_table_songplays,
    insert_data_into_table=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    create_table_sql=SqlQueries.create_table_users,
    insert_data_into_table=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    create_table_sql=SqlQueries.create_table_songs,
    insert_data_into_table=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    create_table_sql=SqlQueries.create_table_artists,
    insert_data_into_table=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    create_table_sql=SqlQueries.create_table_time,
    insert_data_into_table=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
)

end_operator = DummyOperator(
    task_id="Stop_execution",
    dag=dag,
)

# Create task run order
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
