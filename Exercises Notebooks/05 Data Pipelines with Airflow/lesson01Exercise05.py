
import datetime
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

"""------------------------------------------------------------------------
    Lesson 1 Exercise 5 objectives:
    - demonstrate how to use Jinja Templating to retrieve DAG/TASK
execution context at runtime;

    Airflow Macro's Reference: https://bit.ly/2YyXZPF

    This exercise intends to explore and print the contents of almost all
of Airflow Context Variables.
------------------------------------------------------------------------"""

contextDag = DAG(
     dag_id='lesson1Exercise5'
    ,start_date=datetime.datetime(2020,6,11)
    ,end_date=datetime.datetime(2020,6,13)
    ,schedule_interval='@daily'
    ,max_active_runs=1
)

# Dict Unpacking takes place
def logContextVariables(*args,**context):

    #  The list below contains almost all of Airflow Default Context Variables
    # except for those starting with "var."
    contextVariablesList = [
         'ds'
        ,'ds_nodash'
        ,'prev_ds'
        ,'prev_ds_nodash'
        ,'next_ds'
        ,'next_ds_nodash'
        ,'yesterday_ds'
        ,'yesterday_ds_nodash'
        ,'tomorrow_ds'
        ,'tomorrow_ds_nodash'
        ,'ts'
        ,'ts_nodash'
        ,'ts_nodash_with_tz'
        ,'execution_date'
        ,'prev_execution_date'
        ,'prev_execution_date_success'
        ,'prev_start_date_success'
        ,'next_execution_date'
        ,'dag'
        ,'task'
        ,'macros'
        ,'task_instance'
        ,'end_date'
        ,'latest_date'
        ,'ti'
        ,'params'
        ,'task_instance_key_str'
        ,'conf'
        ,'run_id'
        ,'dag_run'
        ,'test_mode'
    ]

    for variable in contextVariablesList:

        # get context variable contents
        contextVariableValue = context[variable]
        # print variable contents
        logging.info(f'"{variable}" variable contains: {contextVariableValue}.')

exploreContextVariables = PythonOperator(
     task_id='exploreContextVariables'
    ,python_callable=logContextVariables
    ,dag=contextDag
    # IMPORTANT: access to context variables must be explicitly enabled
    ,provide_context=True
)




