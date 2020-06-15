
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

"""------------------------------------------------------------------------
    Lesson 1 Exercise 1 demonstrates how to create a simple DAG containing
a single Task and no defined execution schedule.
------------------------------------------------------------------------"""

# define a simple python function to log a short message
def helloWorld():
    # print friendly greeting message
    logging.info("Olar Planeta!")

# create a DAG instance
helloDag = DAG(
    # name DAG instance (how it's gonna show up in the UI)
     dag_id='lesson1Exercise1'
    # set arbitrary start date for demonstration purposes (notice it's not "today's" date)
    ,start_date=datetime.datetime(2020,6,12)
)

# create a Task instance using the PythonOperator Class
greeting = PythonOperator(
    # name Task instance: how its gonna show up in Airflow's UI
     task_id='helloWorld'
    # define python callable object for execution
    ,python_callable=helloWorld
    # attach Task to corresponding DAG
    ,dag=helloDag
)
