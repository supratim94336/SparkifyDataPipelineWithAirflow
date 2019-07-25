from airflow import DAG
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries


# Returns a DAG which creates dimension tables.
def load_dimension_subdag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        *args, **kwargs):
    """
    A python function with arguments, which creates a dag
    :param parent_dag_name: imp ({parent_dag_name}.{task_id})
    :param task_id: imp {task_id}
    :param redshift_conn_id: {any connection id}
    :param args: {verbose}
    :param kwargs: {verbose and context variables}
    :return:
    """
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="users",
        sql_stmt=SqlQueries.user_table_insert,
        mode="insert"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="songs",
        sql_stmt=SqlQueries.song_table_insert,
        mode="insert"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="artists",
        sql_stmt=SqlQueries.artist_table_insert,
        mode="insert"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="time",
        sql_stmt=SqlQueries.time_table_insert,
        mode="insert"
    )

    load_user_dimension_table
    load_song_dimension_table
    load_artist_dimension_table
    load_time_dimension_table

    return dag
