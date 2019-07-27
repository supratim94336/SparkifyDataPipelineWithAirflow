from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,
                               LoadFactOperator,
                               DataQualityOperator)
from airflow.operators.subdag_operator import SubDagOperator
from subdags.subdag_for_dimensions import load_dimension_subdag
from helpers import SqlQueries
from quality_checks.sql_queries import QualityChecks


AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

# set default args
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 1, 1),
    'end_date': datetime(2018, 12, 1),
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': True
}

# dag is complete
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
         )

# dummy for node 0
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# stage events
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    sql_stmt=SqlQueries.log_copy_command,
    provide_context=True,
    json_format="s3://udacity-dend/log_json_path.json"
)

# stage songs
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    sql_stmt=SqlQueries.song_copy_command,
    json_format="auto"
)

# load dimensions
load_dimension_subdag_task = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name="udac_example_dag",
        task_id="load_dimensions",
        redshift_conn_id="redshift",
        start_date=datetime(2018, 1, 1)
    ),
    task_id="load_dimensions",
    dag=dag,
)

# load fact
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_stmt=SqlQueries.songplay_table_insert
)

# run quality check
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt=QualityChecks.count_check,
    tables=['songs', 'time', 'users', 'artists', 'songplays'],
)

# dummy for node end
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


"""
An Overview of the implemented dag:

       --> stage_events --> 
     //                     \\
start                          --> load_facts --> load_dimensions --> quality_check --> end
     \\                     //
       -->  stage_songs -->
"""

# sequence of airflow operations
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_dimension_subdag_task
load_dimension_subdag_task >> run_quality_checks
run_quality_checks >> end_operator
