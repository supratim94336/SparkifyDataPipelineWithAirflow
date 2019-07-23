import datetime
from airflow import DAG
from airflow_home.plugins.operators import (
    FactsCalculatorOperator,
    HasRowsOperator,
    S3ToRedshiftOperator
)
from airflow.operators.postgres_operator import PostgresOperator
from sql import sql_statements
#
# TODO: Create a DAG which performs the following functions:
#
dag = DAG("lesson3.exercise4", start_date=datetime.datetime.utcnow())

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

copy_trips_task = S3ToRedshiftOperator(
    task_id="load_trips_from_s3_to_redshift",
    dag=dag,
    table="trips",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
)

# TODO: Perform a data quality check on the Trips table
check_trips = HasRowsOperator(
    task_id='check_trips_data',
    dag=dag,
    provide_context=True,
    table= 'trips',
    redshift_conn_id= 'redshift'
)

#
# TODO: Use the FactsCalculatorOperator to create a Facts table in
#  RedShift. The fact column should be `tripduration` and the
#  groupby_column should be `bikeid`

calculate_facts = FactsCalculatorOperator(
    task_id="calculate_facts_trips",
    dag=dag,
    redshift_conn_id="redshift",
    origin_table="trips",
    destination_table="trip_facts",
    fact_column="tripduration",
    groupby_column="bikeid"
)
#
# TODO: Define task ordering for the DAG tasks you defined
#
create_trips_table >> copy_trips_task >> check_trips >> calculate_facts