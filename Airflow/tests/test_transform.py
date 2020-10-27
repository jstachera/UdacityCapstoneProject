from unittest import TestCase

from plugins.helpers.common import create_spark_session

from pyspark.sql.dataframe import DataFrame
from plugins.helpers.common import read_data
from pyspark.sql import SparkSession

from tools.i94tools import get_paths
from transform import Transformer


class TestTransform(TestCase):
    _spark: SparkSession

    def setUp(self) -> None:
        self._spark = create_spark_session()

    def tearDown(self) -> None:
        self._spark.stop()

    def _transform_dataset_test(self, isLocal: bool, dataset_name: str, output_format: str):
        paths = list(get_paths(isLocal, dataset_name, output_format))
        transform = Transformer(self._spark, paths[0])
        paths.pop(0)

        if dataset_name == 'country':
            temperature_input_path, country_dict_input_path, output_path = paths
            data = transform.transform_country(country_dict_input_path, temperature_input_path, output_path)
        elif dataset_name == 'airport':
            input_path, output_path = paths
            data = transform.transform_airports(input_path, output_path)
        elif dataset_name == 'us_state':
            demographics_input_path, us_state_dict_input_path, output_path = paths
            data = transform.transform_us_state(us_state_dict_input_path, demographics_input_path, output_path)
        elif dataset_name == 'immigration':
            input_path, date_output_path, img_output_path = paths
            data = transform.transform_immigration(input_path, img_output_path, date_output_path)

        self.assertTrue(data is not None)

    def test_transform_country_local(self):
        self._transform_dataset_test(True, 'country', 'csv')

    def test_transform_country_remote(self):
        self._transform_dataset_test(False, 'country', 'parquet')

    def test_transform_airport_local(self):
        self._transform_dataset_test(True, 'airport', 'csv')

    def test_transform_airport_remote(self):
        self._transform_dataset_test(False, 'airport', 'parquet')

    def test_transform_us_state_local(self):
        self._transform_dataset_test(True, 'us_state', 'csv')

    def test_transform_us_state_remote(self):
        self._transform_dataset_test(False, 'us_state', 'parquet')

    def test_transform_immigration_local(self):
        self._transform_dataset_test(True, 'immigration', 'csv')

    def test_transform_immigration_remote(self):
        self._transform_dataset_test(False, 'immigration', 'parquet')


