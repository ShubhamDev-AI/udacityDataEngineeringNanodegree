
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

"""------------------------------------------------------------------------
    Lesson 1 Exercise 3
    Demonstrate how to set dependencies between Airflow Tasks by chaining
simple Python functions.
------------------------------------------------------------------------"""

# define 4 simple python functions
def helloWorld():
    logging.info("Olar, Planeta!")

def addition():
    logging.info(f"2 + 2 = {2+2}")

def subtraction():
    logging.info(f"6 - 2 = {6-2}")

def division():
    logging.info(f"10 / 2 = {int(10/2)}")

# instantiate DAG Object
dependenciesDag = DAG(
    # set DAG name
     dag_id='lesson1Exercise3'
    # set arbitrary start date 
    ,start_date=datetime.datetime(2020,6,12)
    # schedule daily executions
    ,schedule_interval='@daily'
    # enable backfilling
    ,catchup=True
    # set maximum simultaneous DAG executions to 1
    ,max_active_runs=1
)

# create a single Task for each python function defined above
helloWorldTask = PythonOperator(
     task_id='helloWorld'
    ,python_callable=helloWorld
    ,dag=dependenciesDag
)

additionTask=PythonOperator(
     task_id='addition'
    ,python_callable=addition
    ,dag=dependenciesDag
)

subtractionTask=PythonOperator(
     task_id='subtraction'
    ,python_callable=subtraction
    ,dag=dependenciesDag
)

divisionTask=PythonOperator(
     task_id='division'
    ,python_callable=division
    ,dag=dependenciesDag
)

# set Task dependencies to look like this:
#
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task
#
#   NOTICE Task relations are set by their assigned object names 
# (not by their "task_id" parameters)

# "additionTask" succeeds "helloWorldTask"
helloWorldTask >> additionTask
# "subtractionTask" also succeeds "helloWorldTask" (create "Fork")
helloWorldTask >> subtractionTask
# "additionTask" is succeeded by "divisionTask"
additionTask >> divisionTask
# "subtractionTask" is also succeeded by "divisionTask (Fork reunites)
subtractionTask >> divisionTask


