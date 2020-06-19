
"""----------------------------------------------------------------------------
    Airflow's plugin manager docs: https://bit.ly/2YenFCc
----------------------------------------------------------------------------"""

from airflow.plugins_manager import AirflowPlugin

#   Import the "operators" directory from the same folder in which this 
# "__init__" file resides
import operators

#   Create a Class that encapsulates custom plugins. Notice it inherits from
# Airflow's "AirflowPlugin" Class
class CustomPlugins(AirflowPlugin):

    # MANDATORY: define plugin name (str)
    name = "custom_plugins"

    #   The "operators" list object contains a list of Classes derived from
    # Airflow's "BaseOperator" Class.
    operators = [
         operators.HasRowsOperator
        # ,operators.S3ToRedshiftOperator
    ]