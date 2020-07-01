from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
        Use this Operator to perform a SQL statement against any database table
    and compare its results with user-defined arguments in Python.

        NOTES:
        - In case you want to compare "single_valued_result_query" not to a range
        but to a single value, repeat this value in both "query_result_range_start"
        and "query_result_range_end" parameters.

    :param redshift_conn_id: An ID for an Airflow Connection that's already been
    created either programmatically or via Airflow's CLI or UI.
        It must point to an AWS Redshift database instance.
    :type redshift_conn_id: string

    :param single_valued_result_query: a valid SQL query that yields a single value
    as output. 
        It doesn't matter how complex this query is but it's REQUIRED TO OUTPUT A 
    SINGLE VALUE as result. For example: a date, a timestamp, an integer, etc.
        OUTPUT DATA TYPE: can be any of Postgres' numeric types (INTEGER, FLOAT8, etc)
        or DATE/TIMESTAMP tipes.
    :type single_valued_result_query: string
    
    :param query_result_range_start: minimum value expected (inclusive) as query
    result. It can be either one of the following types:
        - continuous: 3,1415 
        - discrete: 1, 500, n
        - database-valid DATE: '2020-06-30' or '20200630'
    :type query_result_range_start: numeric or string

    :param query_result_range_end: maximum value expected (inclusive) as query
    result. It can be either one of the following types:
        - continuous: 3,1415 
        - discrete: 1, 500, n
        - database-valid DATE: '2020-06-30' or '20200630'
    :type query_result_range_end: numeric or string
    """

    ui_color = '#89DA59'

    template_fields = ("single_valued_result_query",)

    @apply_defaults
    def __init__(
         self
        ,redshift_conn_id=""
        ,single_valued_result_query=""
        ,query_result_range_start=0
        ,query_result_range_end=0
        ,*args
        ,**kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.single_valued_result_query=single_valued_result_query
        self.query_result_range_start=query_result_range_start
        self.query_result_range_end=query_result_range_end

    def execute(self, context):

        # instantiate PostgresHook Class
        pgHookInstance = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # render JINJA templates in case any is present in "single_valued_result_query"
        rendered_single_valued_result_query = self.single_valued_result_query.format(**context)
        # execute SQL query
        dbListOfTuples = pgHookInstance.get_records(rendered_single_valued_result_query)

        #   Check "dbListOfTuples" object length and also the first tuple 
        # returned from the database
        # REMEMBER: "get_records()" returns a list of tuples (psycopg2 underneath)
        if len(dbListOfTuples) < 1 or len(dbListOfTuples[0]) < 1:
            raise ValueError(f'ERROR: SQL query returned no results.')

        # slice twice to get "dbListOfTuples"'s first tuple first element
        dataExtract = dbListOfTuples[0][0]
        # evaluate whether SQL results were between user-specified lower and upper bounds (inclusive)
        # success case
        if self.query_result_range_start <= dataExtract <= self.query_result_range_end:
            self.log.info(f'SUCCESS: SQL query returned "{dataExtract}".')
        # case it fails
        else:
            raise ValueError(f"""ERROR: SQL query returned "{dataExtract}", not between 
                "{self.query_result_range_start}" and "{self.query_result_range_end}".""")

        