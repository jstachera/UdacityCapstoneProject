import sys

from pyspark.sql import SparkSession

# TODO: try/catch import for UI airflow no module found errors
#   Needed some investigation how to handle this cleaner.
try:
    from helpers.common import create_spark_session
    from helpers.transform import Transformer
except ImportError:
    from common import create_spark_session
    from transform import Transformer


class SparkDispatcher:
    _spark: SparkSession

    def __init__(self):
        self._spark = create_spark_session()

    def close(self):
        self._spark.stop()

    def dispath(self, command: str, args: []) -> bool:
        done: bool
        if command == 'process_airport':
            self._validate_command_args(args, command, 3)
            done = self._process_airpot(args)
        elif command == 'process_country':
            self._validate_command_args(args, command, 4)
            done = self._process_country(args)
        elif command == 'process_us_state':
            self._validate_command_args(args, command, 4)
            done = self._process_us_state(args)
        elif command == 'process_immigration':
            self._validate_command_args(args, command, 4)
            done = self._process_immigration(args)
        else:
            raise ValueError(f'SparkDispatcher unknown command: {command}!')

        return done

    def _get_transformer(self, dict_path: str):
        return Transformer(self._spark, dict_path)

    def _validate_command_args(self, args: [], command: str, required_args: int):
        if len(args) != required_args:
            raise ValueError(f'SparkDispatcher {command} command error: invalid arg length!\nrequired: {required_args},'
                             f' got: {len(args)}.\n{str(args)}')

    def _process_immigration(self, args: []):
        if len(args) != 4:
            raise ValueError(f'')

        dict_path = args[0]
        immigration_input_path = args[1]
        immigration_output_path = args[2]
        date_output_path = args[3]

        # transform data
        transformer: Transformer = self._get_transformer(dict_path)
        transformed = transformer.transform_immigration(immigration_input_path, immigration_output_path,
                                                        date_output_path)
        return transformed

    def _process_airpot(self, args: []):
        # extract in/out paths
        dict_path = args[0]
        input_path = args[1]
        output_path = args[2]

        # transform data
        transformer: Transformer = self._get_transformer(dict_path)
        transformed = transformer.transform_airports(input_path, output_path)
        return transformed

    def _process_country(self, args: []):
        # extract in/out paths
        dict_path = args[0]
        country_dict_input_path = args[1]
        temperature_input_path = args[2]
        output_path = args[3]

        # transform data
        transformer: Transformer = self._get_transformer(dict_path)
        transformed = transformer.transform_country(country_dict_input_path,
                                                    temperature_input_path,
                                                    output_path)
        return transformed

    def _process_us_state(self, args: []):
        if len(args) != 4:
            raise ValueError(
                f'SparkDispatcher process country invalid arg length! required at least 4, got: {str(args)}')

        # extract in/out paths
        dict_path = args[0]
        us_state_dict_input_path = args[1]
        demographics_input_path = args[2]
        output_path = args[3]

        # transform data
        transformer: Transformer = self._get_transformer(dict_path)
        transformed = transformer.transform_us_state(us_state_dict_input_path, demographics_input_path, output_path)

        return transformed


if __name__ == "__main__":
    print("Executing spark dispatcher with commad args: {} {} ... {}".format('\n', '\n\t> '.join(sys.argv), '\n'))

    if len(sys.argv) < 2:
        raise ValueError(f'SparkDispatcher invalid number of arguments! required at least 2')

    dispatcher = SparkDispatcher()

    # parse command/args
    command = sys.argv[1]
    args = sys.argv[2:] if len(sys.argv) > 2 else None

    # dispatch command
    print(f'Executing command: {command} ...')
    dispatcher.dispath(command, args)

    # close spark session
    dispatcher.close()

    print(f'Finished executing spark dispatcher for command: {command}.')
