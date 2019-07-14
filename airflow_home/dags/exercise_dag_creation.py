import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os


def greet():
    logging.info("Hello World!")


def cur_time():
    logging.info("Current Time is {}!".format(datetime.datetime.now()))


def cur_dir():
    logging.info("Current dir is {}!".format(os.getcwd()))


def complete():
    logging.info("Now it is time!")

#
# create dag
#


dag = DAG('lesson1.demo2',
          start_date=datetime.datetime.now(),
          catchup=False)

greet_task = PythonOperator(
    task_id="greet_task",
    python_callable=greet,
    dag=dag
)
time_task = PythonOperator(
    task_id="time_task",
    python_callable=cur_time,
    dag=dag
)
dir_task = PythonOperator(
    task_id="dir_task",
    python_callable=cur_dir,
    dag=dag
)
complete_task = PythonOperator(
    task_id="complete_task",
    python_callable=complete,
    dag=dag
)
"""
            ->  time_task ->
          /                  \
greet_task                     complete_task
          \                  /
            ->  dir_task  -> 
"""
greet_task >> time_task
greet_task >> dir_task
complete_task >> time_task
complete_task >> dir_task
