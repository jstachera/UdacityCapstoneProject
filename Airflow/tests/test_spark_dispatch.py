from unittest import TestCase

from spark_dispatch import SparkDispatcher
from tools.i94tools import get_paths


class TestSparkDispatcher(TestCase):
    _dispatcher: SparkDispatcher

    def setUp(self) -> None:
        self._dispatcher = SparkDispatcher()

    def _dispatch_command_test(self, isLocal: bool, command: str, output_format: str):
        paths = list(get_paths(isLocal, command, output_format))

        done = self._dispatcher.dispath(command=command, args=paths)
        self.assertTrue(done)

    def test_dispath_process_airport_command_local(self):
        self._dispatch_command_test(True, 'process_airport', 'csv')

    def test_dispath_process_us_state_command_local(self):
        self._dispatch_command_test(True, 'process_us_state', 'csv')

    def test_dispath_process_immigration_command_local(self):
        self._dispatch_command_test(True, 'process_immigration', 'csv')