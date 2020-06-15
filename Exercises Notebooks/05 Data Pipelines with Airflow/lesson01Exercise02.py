import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

"""------------------------------------------------------------------------
    Lesson 1 Exercise 2
    Expands on Exercise 1 by adding a "schedule_interval" parameter to the
previously created DAG.
------------------------------------------------------------------------"""

# define a simple python function to log a short message
def helloWorld():
    # print friendly greeting message
    logging.info("Olar Planeta!")

# create a DAG instance
helloDag = DAG(
    # name DAG instance (how it's gonna show up in the UI)
     dag_id='lesson1Exercise2'
    # set start_date to a data a month ago
    ,start_date=datetime.datetime.today() - datetime.timedelta(days=3)
    # define daily executions to occur
    ,schedule_interval='@daily'
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