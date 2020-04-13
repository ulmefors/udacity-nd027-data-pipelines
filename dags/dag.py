import configparser
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from helpers import DataQualityTests


config = configparser.ConfigParser()
config.read('redshift.cfg')

S3_BUCKET = 'udacity-dend'
S3_LOG_KEY = 'log_data/{execution_date.year}/{execution_date.month}'
S3_SONG_KEY = 'song_data'
LOG_JSON_PATH = f's3://{S3_BUCKET}/log_json_path.json'
IAM_ROLE_ARN = config['REDSHIFT']['IAM_ROLE_ARN']
REGION = config['REDSHIFT']['REGION']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date':datetime(2018, 11, 1),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG(
    dag_id='dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    table='staging_events',
    s3_bucket=S3_BUCKET,
    s3_key=S3_LOG_KEY,
    iam_role_arn=IAM_ROLE_ARN,
    region=REGION,
    truncate=False,
    json_path=LOG_JSON_PATH,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table='staging_songs',
    s3_bucket=S3_BUCKET,
    s3_key=S3_SONG_KEY,
    iam_role_arn=IAM_ROLE_ARN,
    region=REGION,
    truncate=True,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songplay_table_insert,
    table='songplays',
    truncate=False,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.user_table_insert,
    table='users',
    truncate=True,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.song_table_insert,
    table='songs',
    truncate=True,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.artist_table_insert,
    table='artists',
    truncate=True,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.time_table_insert,
    table='time',
    truncate=True,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    postgres_conn_id='redshift',
    tests=DataQualityTests.tests,
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

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
