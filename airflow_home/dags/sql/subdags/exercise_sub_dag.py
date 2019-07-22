from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow_home.plugins.operators.has_rows import HasRowsOperator
from airflow_home.plugins.operators.s3_to_redshift import S3ToRedshiftOperator


# Returns a DAG which creates a table if it does not exist, and then
# proceeds to load data into that table from S3. When the load is
# complete, a data quality  check is performed to assert that at least
# one row of data is present.
def get_s3_to_redshift_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        create_sql_stmt,
        s3_bucket,
        s3_key,
        *args, **kwargs):
    """
    A python function with arguments, which creates a dag
    :param parent_dag_name: imp ({parent_dag_name}.{task_id})
    :param task_id: imp {task_id}
    :param redshift_conn_id: {any connection id}
    :param aws_credentials_id: {aws credentials}
    :param table: {table name}
    :param create_sql_stmt: {create table sql query}
    :param s3_bucket: {s3 bucket}
    :param s3_key: {key for the bucket to get year and month}
    :param args: {verbose}
    :param kwargs: {verbose and context variables}
    :return:
    """
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    create_task = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=create_sql_stmt
    )

    copy_task = S3ToRedshiftOperator(
        task_id=f"load_{table}_from_s3_to_redshift",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key
    )

    check_task = HasRowsOperator(
        task_id=f"check_{table}",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id
    )
    create_task >> copy_task
    copy_task >> check_task

    return dag
