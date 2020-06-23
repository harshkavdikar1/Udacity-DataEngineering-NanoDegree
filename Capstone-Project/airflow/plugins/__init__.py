from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.ExtractionFromSASOperator,
        operators.CreateTableOperator,
        operators.CopyTableOperator,
        operators.CheckQualityOperator,
        operators.InsertTableOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
