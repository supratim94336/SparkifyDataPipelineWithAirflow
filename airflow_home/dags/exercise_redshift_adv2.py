# Instructions
# 1 - Modify the bikeshare DAG to load data month by month, instead of loading it all at once, every time.
# 2 - Use time partitioning to parallelize the execution of the DAG.

import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from .sql import sql_statements


# task 1: load trip data to redshift
# aws_hook - cause we need aws credentials
# redshift_hook - cause we need the cluster where we will
# copy
# execution_date - this is a context variable (also a
# timestamp type of kind)
# sql_stmt - we are using partitioned data here. So,
# in s3 storage, the data is partitioned as year/month
# so, we are providing in copy statement year and month
def load_trip_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    # sql statement
    sql_stmt = sql_statements.COPY_ALL_TRIPS_SQL.format(
        credentials.access_key,
        credentials.secret_key,
    )
    redshift_hook.run(sql_stmt)


# task 2: load station data to redshift
def load_station_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql_statements.COPY_STATIONS_SQL.format(
        credentials.access_key,
        credentials.secret_key,
    )
    redshift_hook.run(sql_stmt)


# create the dag
# here start_date, end_date, schedule_interval is verbose
# max_active_runs = 1 means parallely one run of the
# whole task will be running
dag = DAG(
    'lesson2.exercise5',
    start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2018, 2, 1, 0, 0, 0, 0),
    max_active_runs=1
)

# dag task 1: table creation of trips
create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

# dag task 2: copy to trips
# In the dag you have to provide:
# task_id,
# provide_context = True (will ensure macro from airflow
# can be used which is passed as a string argument in
# **kwargs)
copy_trips_task = PythonOperator(
    task_id='load_trips_from_s3_to_redshift',
    dag=dag,
    python_callable=load_trip_data_to_redshift,
    # TODO: ensure that we provide context to our Python Operator
    provide_context=True

)

# dag task 3: create table for stations
# In the dag you have to provide:
# task_id,
# postgres_conn_id,
# sql
create_stations_table = PostgresOperator(
    task_id="create_stations_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
)

copy_stations_task = PythonOperator(
    task_id='load_stations_from_s3_to_redshift',
    dag=dag,
    python_callable=load_station_data_to_redshift,
)

create_stations_traffic_table = PostgresOperator(
    task_id="create_stations_traffic_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRAFFIC_TABLE_SQL
)

insert_data_into_traffic = PostgresOperator(
    task_id='insert_data_into_traffic',
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.TRAFFIC_INSERTION_SQL#.format('{{ next_ds }}', '{{ prev_ds }}', '{{ next_ds }}', '{{ prev_ds }}')
)

create_stations_traffic_table >> create_trips_table >> copy_trips_task >> insert_data_into_traffic
create_stations_traffic_table >> create_stations_table >> copy_stations_task >> insert_data_into_traffic
