from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Use this Operator to perform a SQL COUNT() function based on user input.

    :param redshift_conn_id: An ID for an Airflow Connection that's already been
    created either programmatically or via Airflow's CLI or UI.
        It must point to an AWS Redshift database instance.
    :type redshift_conn_id: string

    :param schema: 'redshift_conn_id' schema within which 'table' is located.
    :type schema: string
    
    :param table: name of the table designated for data quality check. 
    :type table: string

    :param column: name of the column for rows to be counted on. Defaults to "*"
    (asterisk). A SQL COUNT() function will receive this parameter
    :type column: string
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
         self
        ,redshift_conn_id=""
        ,sql_statement=""
        ,schema=""
        ,table=""
        ,column="*"
        ,*args
        ,**kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.schema=schema
        self.table=table
        self.column=column

    def execute(self, context):

        # instantiate PostgresHook Class
        pgHookInstance = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # append schema and table prior to SQL execution
        schemaDotTable = self.schema + "." + self.table

        dbListOfTuples = pgHookInstance.get_records(f"""
            SELECT
                COUNT({self.column})
            FROM
                {schemaDotTable}
            ;
        """)

        #   Check "dbListOfTuples" object length and also the first tuple 
        # returned from the database
        # REMEMBER: "get_records()" returns a list of tuples (psycopg2 underneath)
        if len(dbListOfTuples) < 1 or len(dbListOfTuples[0]) < 1:
            raise ValueError(f'ERROR: {schemaDotTable} returned no results.')

        # slice twice to get "dbListOfTuples"'s first tuple first element
        rowCountExtract = dbListOfTuples[0][0]
        # check table row count is greater than zero
        if rowCountExtract < 1:
            raise ValueError(f'ERROR: {schemaDotTable} contains ZERO ROWS.')

        self.log.info(f'SUCCESS: COUNT({self.column}) on {schemaDotTable} returned {rowCountExtract} rows.')