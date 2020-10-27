from unittest import TestCase

from pyspark.sql import SparkSession

from plugins.helpers.cleaning import Cleaner
from plugins.helpers.common import create_spark_session, read_data
from tools.i94tools import get_paths


class TestClean(TestCase):
    _spark: SparkSession

    def setUp(self) -> None:
        self._spark = create_spark_session()

    def tearDown(self) -> None:
        self._spark.stop()

    def _clean_dataset_test(self, isLocal: bool, dataset_name: str):
        paths = get_paths(isLocal, dataset_name, 'csv')
        dict_path = paths[0]
        cleaner = Cleaner(dict_path)
        data = read_data(self._spark, paths[1])

        if dataset_name == 'temperature':
            data = cleaner.clean_temperature_data(data)
        elif dataset_name == 'airport':
            data = cleaner.clean_airport_data(data)
        elif dataset_name == 'us_state':
            data = cleaner.clean_demographic_data(data)
        elif dataset_name == 'immigration':
            data = cleaner.clean_immigration_data(data)

        self.assertTrue(data is not None)

    def test_clean_temperature_data_remote(self):
        self._clean_dataset_test(False, 'temperature')

    def test_clean_temperature_data_local(self):
        self._clean_dataset_test(True, 'temperature')

    def test_clean_airport_data_remote(self):
        self._clean_dataset_test(False, 'airport')

    def test_clean_airport_data_local(self):
        self._clean_dataset_test(True, 'airport')

    def test_clean_us_state_data_local(self):
        self._clean_dataset_test(True, 'us_state')

    def test_clean_us_state_data_remote(self):
        self._clean_dataset_test(False, 'us_state')

    def test_clean_immigration_data_local(self):
        self._clean_dataset_test(True, 'immigration')

    def test_clean_immigration_data_remote(self):
        self._clean_dataset_test(False, 'immigration')
