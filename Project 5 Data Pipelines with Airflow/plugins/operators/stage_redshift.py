from airflow.contrib.hooks.aws_hook impor AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# StageJsonFromS3ToRedshift
class StageJsonFromS3ToRedshift(BaseOperator):
    ui_color = '#358140'

    """

    :param aws_region: 
    :type aws_region: 

    :param redshift_conn_id: 
    :type redshift_conn_id: 

    :param aws_credentials_id: 
    :type aws_credentials_id: 

    :param bucket_name: 
    :type bucket_name: 

    :param s3_key: 
    :type s3_key: 

    :param schema_dot_table: 
    :type schema_dot_table: 

    :param jsonpaths_file: 
    :type jsonpaths_file: 


    :param truncate: 
    :type truncate: bool

    :param : 
    :type : 
    """

    #   Define tuple with a list of all JINJA templatable variables within this
    # Class
    template_fields = ("s3_key")

    @apply_defaults
    def __init__(
         self
        ,aws_region="us-west-2"
        ,redshift_conn_id=""
        ,aws_credentials_id="aws_default"
        ,bucket_name=""
        ,s3_key=""
        ,schema_dot_table=""
        ,jsonpaths_file="auto"
        ,truncate=False
        ,*args
        ,**kwargs
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.bucket_name=bucket_name
        self.s3_key=s3_key
        self.schema_dot_table=schema_dot_table
        self.jsonpaths_file=jsonpaths_file
        self.truncate=truncate

    def execute(self, context):

        # instantiate AwsHook Class
        awsHookInstance = AwsHook(self.aws_credentials_id)

        # fetch AWS credentials with "get_credentials()" method
        awsCredentials = awsHookInstance.get_credentials()

        # instantiate PostgresHook Class
        pgHookInstance = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # evaluate "truncate" parameter and perform corresponding action
        if self.truncate == True: 
            # inform current action
            self.log.info(f"Executing TRUNCATE statement on {self.schema_dot_table}.")
            # truncate target table using PostgresHook's "run()" method 
            pgHookInstance.run(f"TRUNCATE TABLE {self.schema_dot_table}")

        # render JINJA templates within "s3_key" variable
        rendered_file_key = self.s3_key.format(**context)

        # concatenate S3 bucket name to file/object key
        complete_s3_path = f"s3://{self.bucket_name}/{rendered_file_key}"

        self.log.info(f"Bulk Loading from {complete_s3_path}.")

        pgHookInstance.run(f"""
            COPY 
                {self.schema_dot_table}
            FROM
                '{complete_s3_path}'
            FORMAT
                -- use a "jsonpaths_file" to map source-to-destination columns
                -- use "auto" to match source-to-destination by exact column names
                JSON AS '{self.jsonpaths_file}'
            ACCESS_KEY_ID
                '{awsCredentials.access_key}'
            SECRET ACCESS KEY
                '{awsCredentials.secret_key}'
            REGION
                '{self.aws_region}'
            COMPUPDATE OFF
            -- make sure empty strings doesn't disrupt the load process
            BLANKSASNULL
        """
        )





