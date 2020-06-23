import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class FactsCalculatorOperator(BaseOperator):

    """------------------------------------------------------------------------

    ------------------------------------------------------------------------"""

    #define aggregation ("fact generator") sql template
    fact_sql_template = """
        -- drop table in case it already exists
        DROP TABLE IF EXISTS {destination_table};

        CREATE TABLE {destination_table} AS
            SELECT
                 {group_by_column}
                ,MIN({fact_column}) AS min_{fact_column} 
                ,AVG({fact_column}) AS avg_{fact_column}
                ,MAX({fact_column}) AS max_{fact_column}
            FROM
                {origin_table}
            GROUP BY
                {group_by_column}
        ;
    """

    @apply_defaults
    def __init__(
         self
        ,redshift_conn_id=""
        ,origin_table=""
        ,destination_table=""
        ,fact_column=""
        ,group_by_column=""
        ,*args
        ,**kwargs
    ):

        super(FactsCalculatorOperator, self).__init__(*args,**kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.origin_table=origin_table
        self.destination_table=destination_table
        self.fact_column=fact_column
        self.group_by_column=group_by_column

    def execute(self, context):

        # instantiate PostgresHook
        derivedRedshiftHook = PostgresHook(
            postgres_conn_id=self.redshift_conn_id
        )

        # format template SQL statement
        formattedSql = FactsCalculatorOperator.fact_sql_template.format(
             origin_table=self.origin_table
            ,destination_table=self.destination_table
            ,fact_column=self.fact_column
            ,group_by_column=self.group_by_column
        )

        # execute SQL statement
        derivedRedshiftHook.run(formattedSql)

