
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# create a Class that inherits from Airflow's BaseOperator
class HasRowsOperator(BaseOperator):

    """------------------------------------------------------------------------
        This Class implements a functionality that counts rows in a given
    table from a redshift connection.
    ------------------------------------------------------------------------"""

    @apply_defaults
    def __init__(
         self
        ,redshift_conn_id=""
        ,table=""
        ,*args
        ,**kwargs
    ):

        super(HasRowsOperator, self).__init__(*args,**kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table

    # REMEMBER: All Operators must define an "execute()" method
    def execute(self, context):

        # instantiate Postgres Hook
        derivedRedshiftHook = PostgresHook(self.redshift_conn_id)

        queryResults = derivedRedshiftHook.get_records(
            f"SELECT COUNT(*) FROM {self.table}"
        )

        # check "queryResults" object length
        if len(queryResults) < 1 or len(queryResults[0]) < 1:
            raise ValueError(f'Data Quality check failed. {self.table} returned no results.')

        # check "queryResults" object actual content
        queryResultsContent = queryResults[0][0]

        if queryResultsContent <1:
            raise ValueError(f'Data Quality check failed. {self.table} contains zero rows.')

        # If all tests passed with no errors, print a success message
        logging.info(f'Data Quality check passed: {self.table} returned {queryResultsContent} entries.')