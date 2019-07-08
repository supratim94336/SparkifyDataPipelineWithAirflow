import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os

#
# TDO: Define a function for python operator to call
#

# src = '/usr/bin/python'
# dst = '/tmp/subdir/python'
#
# if not os.path.isdir(os.path.dirname(dst)):
#     os.makedirs(os.path.dirname(dst))
# os.symlink(src, dst)


def greet():
    logging.info("Hello World!")


dag = DAG('lesson1.demo1', start_date=datetime.datetime.now())

#
# create dag
#

greet_task = PythonOperator(
    task_id="greet_task",
    python_callable=greet,
    dag=dag
)
