from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

# TODO: try/catch import for UI airflow no module found errors
#   Needed some investigation how to handle this cleaner.
from helpers import SqlQueries, SqlChecks, map_columns, print_data_info, cast_columns, convert_columns
from helpers import Cleaner
from helpers import Transformer
from helpers import SparkDispatcher
from helpers import read_dicts
from operators import StageToRedshiftOperator, DataQualityOperator


# Defining the plugin class
class I94Plugin(AirflowPlugin):
    name = "i94_plugin"
    operators = [
        StageToRedshiftOperator,
        DataQualityOperator
    ]
    helpers = [
        SqlQueries,
        SqlChecks,
        read_dicts,
        map_columns,
        print_data_info,
        cast_columns,
        convert_columns,
        Cleaner,
        Transformer,
        SparkDispatcher
    ]


