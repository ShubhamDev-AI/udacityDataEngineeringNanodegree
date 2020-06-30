from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageJsonToRedshiftOperator(BaseOperator):
    """
    Use this Operator to ingest JSON files stored on S3 into a Redshift database
    instance.
    
    :param aws_region: AWS Region where services used by this Class are located.
    :type aws_region: string

    :param redshift_conn_id: An ID for an Airflow Connection that's already been
    created either programmatically or via Airflow's CLI or UI.
        It must point to an AWS Redshift database instance.
    :type redshift_conn_id: string

    :param aws_credentials_id: An ID for an Airflow Connection that's already been
    created either programmatically or via Airflow's CLI or UI.
        It must point to a pair of AWS's ACCESS_KEY and SECRET_ACCESS_KEY.
    :type aws_credentials_id: string

    :param bucket_name: S3 Bucket name where data to be bulk loaded is located.
    :type bucket_name: string

    :param s3_key: path within 'bucket_name' leading to a valid JSON file.
    :type s3_key: string

    :param schema: 'redshift_conn_id' schema within which 'table' is located.
    :type schema: string
    
    :param table: name of the table targeted by 'sql_insert_statement'. 
    :type table: string

    :param jsonpaths_file: A valid path leading to a file containing the path
    to be used within 's3_key' parameter's file.
    :type jsonpaths_file: string

    :param truncate: a boolean value indicating whether 'table' must
    be truncated prior to being loaded.
    :type truncate: bool

    :param create_table_statement: A SQL CREATE TABLE statement for 'table'
    parameter. The two DDL statements below are executed before prior to
    'create_table_statement' being executed:
        1. CREATE SCHEMA IF NOT EXISTS 'schema';
        2. DROP TABLE IF EXISTS 'schema'.'table';
    :type create_table_statement: string
    """

    ui_color = '#358140'

    #   Define tuple with a list of all JINJA templatable variables within this
    # Class
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(
         self
        ,aws_region="us-west-2"
        ,redshift_conn_id=""
        ,aws_credentials_id="aws_default"
        ,bucket_name=""
        ,s3_key=""
        ,schema=""
        ,table=""
        ,jsonpaths_file="auto"
        ,truncate=False
        ,create_table_statement=""
        ,*args
        ,**kwargs
    ):

        super(StageJsonToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_region=aws_region
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.bucket_name=bucket_name
        self.s3_key=s3_key
        self.schema=schema
        self.table=table
        self.jsonpaths_file=jsonpaths_file
        self.truncate=truncate
        self.create_table_statement=create_table_statement

    def execute(self, context):

        # instantiate AwsHook Class
        awsHookInstance = AwsHook(self.aws_credentials_id)
        # fetch AWS credentials with "get_credentials()" method
        awsCredentials = awsHookInstance.get_credentials()
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

        # evaluate "truncate" parameter and perform corresponding action
        if self.truncate == True: 
            # inform current action
            self.log.info(f"Executing TRUNCATE statement on {self.schema}.{self.table}.")
            # truncate target table using PostgresHook's "run()" method 
            pgHookInstance.run(f"TRUNCATE TABLE {self.schema}.{self.table};")
        else:
            self.log.info("TRUNCATE statement NOT EXECUTED (see 'truncate' parameter).")

        # render JINJA templates within "s3_key" variable
        rendered_file_key = self.s3_key.format(**context)

        # concatenate S3 bucket name to file/object key
        complete_s3_path = f"s3://{self.bucket_name}/{rendered_file_key}"

        self.log.info(f"Bulk Loading {complete_s3_path} into {self.schema}.{self.table}.")

        pgHookInstance.run(f"""
            COPY 
                {self.schema}.{self.table}
            FROM
                '{complete_s3_path}'
            FORMAT
                -- use a "jsonpaths_file" to map source-to-destination columns
                -- use "auto" to match source-to-destination by exact column names
                JSON AS '{self.jsonpaths_file}'
            ACCESS_KEY_ID
                '{awsCredentials.access_key}'
            SECRET_ACCESS_KEY
                '{awsCredentials.secret_key}'
            REGION
                '{self.aws_region}'
            COMPUPDATE OFF
            -- make sure empty strings doesn't disrupt the load process
            BLANKSASNULL
        """
        )





