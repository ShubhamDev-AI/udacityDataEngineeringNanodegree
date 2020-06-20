import datetime
from airflow import DAG
from airflow.operators import (
     S3ToRedshiftOperator
    ,HasRowsOperator
    ,PostgresOperator
)

def redshift_table_ddl_plus_bulk_load(
     parent_dag_id
    ,parent_task_id
    ,redshift_conn_id
    ,aws_credentials_id
    ,table
    ,create_table_statement
    ,s3_bucket
    ,s3_key
    ,*args
    ,**kwargs
):

    """
        This Python Factory Function returns an Airflow DAG Class instance to
    be used as a SubDag within the (outer) DAG that calls it.

    The DAG in turn executes three Task within a Redshift database instance:
        - execute CREATE TABLE (DDL) statement through PostgresOperator;
        - execute COPY (bulk load) to pull data from S3 into the newly created
    table through custom S3ToRedshiftOperator
        - perform data quality check through custom HasRowsOperator

    Args:
        parent_dag_id (str): 

    """

    # instantiate DAG Class
    subDagInstance = DAG(
        #----------------------------------------------------------------------
        #
        #   ATTENTION! "parent_dag_id.parent_task_id" IS MANDATORY
        #
        #   It's an Airflow Convention that subdags must have their "dag_id" 
        # attribute set to the combination of:
        #
        #   1. "dag_id" of the outer DAG Instance calling the SubDagOperator
        # which in turn calls the Python Factory Function; 
        #
        #   2. "task_id" assigned to the SubDagOperator mentioned in item number 1
        #
        #   3. "." a dot must separate  the "dag_id" and "task_id" mentioned in
        # both items 1 and 2, respectively;
        #
        #----------------------------------------------------------------------
         dag_id=f"{parent_dag_id}.{parent_task_id}"
        # make sure keyword arguments are also received
        ,**kwargs
    )

    # NOTICE how Task instances are created in a SubDag just as they would in
    # a "normal" DAG.
    createTable = PostgresOperator(
         task_id=f'create_{table}_table'
        ,postgres_conn_id=redshift_conn_id
        ,sql=create_table_statement
        ,dag=subDagInstance
    )

    bulkLoadTable = S3ToRedshiftOperator(
         task_id=f'bulk_load_{table}_table'
        ,redshift_conn_id=redshift_conn_id
        ,aws_credentials_id=aws_credentials_id
        ,table=table
        ,s3_bucket=s3_bucket
        ,s3_key=s3_key
        ,dag=subDagInstance
    )

    checkDataQuality = HasRowsOperator(
         task_id=f'check_{table}_table'
        ,redshift_conn_id=redshift_conn_id
        ,table=table
        ,dag=subDagInstance
    )

    # Define Task dependencies
    createTable >> bulkLoadTable >> checkDataQuality

    return subDagInstance

