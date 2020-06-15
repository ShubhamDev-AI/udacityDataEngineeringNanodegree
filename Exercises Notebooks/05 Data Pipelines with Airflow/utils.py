
import configparser
#from airflow.models import Variable

# instantiate "configparser" Class
config = configparser.ConfigParser()
# read config file contents
config.read_file('beware_credentials.cfg')

s3_prefix=config.get('aws_credentials','s3_prefix')

print(s3_prefix)