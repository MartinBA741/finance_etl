from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

import configparser
config = configparser.ConfigParser()

AWS_KEY = config['AWS']['ID']
AWS_SECRET = config['AWS']['KEY']

default_args = {'owner': 'udacity',
                'Depends_on_past': False,
                'wait_for_downstream': True,
                'start_date': datetime(2019, 1, 12),
                'end_date': datetime(2021, 1, 12),
                'max_active_runs': 1,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 3,
                'retry_delay': timedelta(minutes=5),
                'catchup': False,
                }

dims_load_type = 'append'

dag = DAG('data_pipeline_dag',
            catchup=False,
            default_args=default_args,
            description='Load and transform data in Redshift with Airflow',
            schedule_interval= '@weekly' #'@hourly' #'@daily' #'0 * * * *'
            )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# stage_events_to_redshift
stage_hist_to_redshift = StageToRedshiftOperator(    
    task_id='Stage_events',
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="stocks",
    s3_key="hist_data",
    region="us-east-1",
    dag=dag
)

#stage_songs_to_redshift
stage_recent_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="stocks",
    s3_key="recent_data",
    region="us-east-1",
    dag=dag
)

# load_songplays_table
load_hist_table = LoadFactOperator(
    task_id='Load_hist_fact_table',
    redshift_conn_id="redshift",
    table="hist",
    column_list=['playid', 'start_time', 'userid', 'level', 'songid', 'artistid', 'sessionid', 'location', 'user_agent'],
    select_sql=SqlQueries.hist_table_insert ,
    dag=dag
)

#load_user_dimension_table = LoadDimensionOperator(
#    task_id='Load_user_dim_table',
#    redshift_conn_id="redshift",
#    table="users",
#    column_list=['userid', 'first_name', 'last_name', 'gender', 'level'],
#    select_sql=SqlQueries.user_table_insert,
#    truncate_insert=True,
#    dag=dag
#)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    column_list=['start_time', 'hour', 'day', 'week', 'month','year','weekday'],
    select_sql=SqlQueries.time_table_insert,
    truncate_insert=True,
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tables_list=['songplays','songs','artists','time','users'],
    provide_context=True,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Configure Task Dependencies
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

load_songplays_table << [stage_events_to_redshift,
                        stage_songs_to_redshift]

load_songplays_table >> [load_song_dimension_table, 
                        load_user_dimension_table, 
                        load_artist_dimension_table, 
                        load_time_dimension_table]

run_quality_checks << [load_song_dimension_table,
                        load_user_dimension_table,
                        load_artist_dimension_table,
                        load_time_dimension_table]

run_quality_checks >> end_operator