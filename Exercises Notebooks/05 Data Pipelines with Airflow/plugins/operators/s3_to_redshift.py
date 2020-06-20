from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class S3ToRedshiftOperator(BaseOperator):

    """------------------------------------------------------------------------
        BULK LOAD DATA FROM AWS S3 TO REDSHIFT DATABASE

        Parameters:

        redshift_conn_id (str): Airflow's Connection ID to Redshift instance;

        aws_credentials_id (str): Airflow's Connection ID to AWS' services;
        
        table (str): table name within Redshift instance to manipulate;

        truncate (bool): Indicate whether the table should be truncated
    prior to being loaded;

        s3_bucket (str): S3 Bucket name from which data will be pulled;

        s3_key (str): String containing additional path structure within defined
    S3 Bucket (complements "s3_key");

        delimiter (str): source file delimiter character (e.g: ',' for CSV files)

        ignore_headers (int): number of rows Redshift's COPY statement must
    ignore because they actually contain table headers.
      
    ------------------------------------------------------------------------"""

    # IMPORTANT:
    # create variable that determines JINJA templatable fields 
    # elements listed in this variable will be allowed to interact with 
    # Airflow Template Variables listed here: https://bit.ly/2Z7Ykt5
    #
    # NOTES:
    # The contents of these "templated_fields" can be seen in Airflow's UI
    # under the following options path:
    #
    # Dag Instance -> Task Instance -> Rendered -> Rendered Templates
    #
    template_fields = ("s3_key",)

    #   create variable that holds a standard Redshift COPY statement for later
    # use within this Class.
    copy_sql = """
        -- which table is gonna receive data?
        COPY {}
        -- where is the data comming from?
        FROM '{}'
        -- AWS "login"
        ACCESS_KEY_ID '{}'
        -- AWS "password"
        SECRET_ACCESS_KEY '{}'
        -- additional source file related parameters
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    # call "apply_defaults" decorator and initialize Class' methods and attributes
    #
    # NOTES: 
    #   - the "truncate" parameter's been deliberately added to provide additional
    # flexibility when loading data. An ideia is to use it in conjunction with
    # JINJA template variables to programmatically evaluate whether an execution
    # should be considered "historical" or "incremental" data loads;
    #   - Airflow threw an error (a Python Syntax error) when the "truncate"
    # attribute was called within the "execute" function without having
    # previously received it corresponding user input (it was not being mentioned
    # within this Class' "__init__" function);
    @apply_defaults
    def __init__(
         self
        ,redshift_conn_id=""
        ,aws_credentials_id=""
        ,table=""
        # default "truncate" behaviour: False (do not truncate)
        ,truncate=False
        ,s3_bucket=""
        ,s3_key=""
        # delimiter defaults to "," (comma) when no user input is received
        ,delimiter=","
        # default "ignore_headers" behaviour: True
        ,ignore_headers=1
        ,*args
        ,**kwargs
    ):

        #   IMPORTANT: also initialise "BaseOperator", from which the Class
        # being defined inherits.
        super(S3ToRedshiftOperator, self).__init__(*args,**kwargs)

        # set Class' attributes
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.truncate=truncate
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.delimiter=delimiter
        self.ignore_headers=ignore_headers

    def execute(self, context):

        """--------------------------------------------------------------------
            REMEMBER: ALL OPERATORS MUST HAVE AN "execute()" METHOD

            This method performs the following actions on a Postgres/Redshift
        database instance:

            - TRUNCATE target table
            - LOAD target table

        --------------------------------------------------------------------"""
        # instantiate AwsHook
        awsHookInstance = AwsHook(self.aws_credentials_id)
        # user "get_credentials()" method to retrieve and store AWS credentials
        awsCredentials = awsHookInstance.get_credentials()
        # instantiate PostgresHook and set its Airflow Connection 
        derivedRedshiftHook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # evaluate "truncate" parameter and perform corresponding action
        if self.truncate == True: 
            # inform current action
            self.log.info(f"Executing TRUNCATE statement on {self.table}.")
            # truncate target table using PostgresHook's "run()" method 
            derivedRedshiftHook.run(f"TRUNCATE TABLE {self.table}")
        
        self.log.info("Bulk Loading data from S3 to Redshift.")
        # render additional S3 Bucket Paht ("folders" within bucket, etc)
        blobStorageKey = self.s3_key.format(**context)

        # concatenate S3 Bucket Path
        completeS3Path = f"s3://{self.s3_bucket}/{blobStorageKey}"

        # "format()" the "copy_sql" variable set at this Class' top
        # NOTES:
        #   - the Class' name MUST precede its variables;
        #   - trying to use "copy_sql" withou "S3ToRedshiftOperator"
        # preceding it threw a "name not defined" error ("copy_sql"
        # wasn't defined within "execution" function scope).

        formattedCopySql = S3ToRedshiftOperator.copy_sql.format(
            # which table is gonna receive the data?
             self.table
            # where's the data comming from?
            ,completeS3Path
            # AWS login
            ,awsCredentials.access_key
            # AWS password
            ,awsCredentials.secret_key
            # source file settings: pass user-defined Class parameters
            ,self.ignore_headers
            ,self.delimiter
        )

        # use PostresHook's "run()" method to execute SQL COPY statement
        derivedRedshiftHook.run(formattedCopySql)





    