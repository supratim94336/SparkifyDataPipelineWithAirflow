import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


def greet():
    logging.info("Hello World!")


dag = DAG('lesson1.demo1', start_date=datetime.datetime.now(), catchup=False)

#
# create dag
#
dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
greet_task = PythonOperator(
    task_id="greet_task",
    python_callable=greet,
    dag=dag
)
dummy_operator >> greet_task
