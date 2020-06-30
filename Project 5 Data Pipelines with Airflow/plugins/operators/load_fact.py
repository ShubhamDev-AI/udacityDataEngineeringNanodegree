
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

class LoadFactOperator(BaseOperator):
    """
        Use this Operator to perform an INSERT SQL statement into a FACT table 
    located in AWS Redshift.

    :param redshift_conn_id: An ID for an Airflow Connection that's already been
    created either programmatically or via Airflow's CLI or UI.
        It must point to an AWS Redshift database instance.
    :type redshift_conn_id: string

    :param create_table_statement: A SQL CREATE TABLE statement for 'table'
    parameter. The two DDL statements below are executed before prior to
    'create_table_statement' being executed:
        1. CREATE SCHEMA IF NOT EXISTS 'schema';
        2. DROP TABLE IF EXISTS 'schema'.'table' CASCADE;
    :type create_table_statement: string

    :param sql_insert_statement: A string containing one or more SQL INSERT 
    statements separated by ';' (semi-colon).
    :type sql_insert_statement: string

    :param schema: 'redshift_conn_id' schema within which 'table' is located.
    :type schema: string
    
    :param table: name of the table targeted by 'sql_insert_statement'. 
    :type table: string

    :param truncate: a boolean value indicating whether 'table' must
    be truncated prior to being loaded.
    :type truncate: bool
    """

    # Define Airflow's UI color for Tasks created from "LoadFactOperator" Class
    ui_color = '#F98866'

    #   Define tuple with a list of all JINJA templatable variables within this
    # Class
    #template_fields = ("sql_insert_statement")

    @apply_defaults
    def __init__(
         self
        ,redshift_conn_id=""
        ,create_table_statement=None    
        ,sql_insert_statement=""  
        ,schema=""
        ,table=""
        ,truncate=False        
        ,*args
        ,**kwargs
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.create_table_statement=create_table_statement
        self.sql_insert_statement=sql_insert_statement
        self.schema=schema
        self.table=table
        self.truncate=truncate

    def execute(self, context):

        #----------------------------------------------------------------------
        #
        # IMPORTANT REMINDER:
        #
        #   By performing both DDL and DML statements, this operator intentionally
        # violates the best practice of executing a single activity per Operator
        # call.
        #   It was an intentional move to abide to Udacity's required Task 
        # dependencies for this Project.
        #
        #----------------------------------------------------------------------

        # instantiate PostgresHook Class
        pgHookInstance = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #   Evaluate whether DDL statements must be executed before table load
        # takes place.
        if self.create_table_statement is not None:
            # inform DDL operations are taking place
            self.log.info(f"DDL EXECUTED: creating {self.schema} schema and {self.table} table.")
            # execute DDL statements to make 2 things sure:
            # 1. destination schema exists within 'redshift_conn_id'
            # 2. drop destination table if it already exists in destination schema.
            pgHookInstance.run(f"""
                CREATE SCHEMA IF NOT EXISTS {self.schema};

                DROP TABLE IF EXISTS {self.schema}.{self.table} CASCADE;
                -- also execute received create table statement
                {self.create_table_statement}
            """)
        else:
            self.log.info("DDL statements NOT EXECUTED (see 'create_table_statement' parameter).")

        # evaluate whether TRUNCATE statement must be executed.
        if self.truncate == True:
            # inform 'truncate' parameter invoked
            self.log.info(f"TRUNCATE '{self.schema}.{self.table}' EXECUTED (see 'truncate' parameter).")
            # execute TRUNCATE statement
            pgHookInstance.run(f"""
                TRUNCATE TABLE {self.schema}.{self.table};
            """)
        else:
            # inform 'truncate' parameter NOT invoked
            self.log.info(f"TRUNCATE statement NOT EXECUTED (see 'truncate' parameter).")

        self.log.info(f"Executing INSERT statement on {self.schema}.{self.table}.")

        # make sure JINJA templates are rendered before execution
        #rendered_dml_statement = self.sql_insert_statement.format(**context)

        # execute SQL INSERT statement
        pgHookInstance.run(self.sql_insert_statement)

        
