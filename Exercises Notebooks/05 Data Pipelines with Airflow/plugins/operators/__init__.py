
"""----------------------------------------------------------------------------
    IMPORT PYTHON SCRIPTS

    From the directory in which this "__init__" file is located.
----------------------------------------------------------------------------"""

from operators.has_rows import HasRowsOperator
from operators.s3_to_redshift import S3ToRedshiftOperator

# set "__all__" attribute
__all__ = [
     'HasRowsOperator'
    ,'S3ToRedshiftOperator'
]
