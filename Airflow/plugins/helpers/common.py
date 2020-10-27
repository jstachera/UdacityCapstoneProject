import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

import pandas as pd

"""
    Common i94project functions.
"""


def create_spark_session() -> SparkSession:
    """
        Creates spark session.

        :return: spark session
    """
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages',
                'org.apache.hadoop:hadoop-aws:3.2.0,saurfang:spark-sas7bdat:3.0.0-s_2.12') \
        .enableHiveSupport() \
        .getOrCreate()

    # setting spark context writing
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')
    sc._jsc.hadoopConfiguration().set('mapreduce.fileoutputcommitter.algorithm.version', '2')
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", r"")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", r"")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                         "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    return spark


def print_data_info(data: DataFrame, file_name: str = '', isDetailed: bool = False):
    """
        Prints spark i94project frame description

        :param isDetailed:
        :param file_name:
        :param data: spark i94project frame
        :return: none
    """
    # if verbose_mode:
    print('----------------------------------------')
    print(f'\r| Data {file_name} info:')
    print('\r| Schema')
    data.printSchema()
    if isDetailed:
        print('\r| Types')
        print(data.dtypes)
        # print('\r| Describe')
        # print(data.describe().show())
        print('\r| First rows')
        data.show(n=10)
        print('\r| Row count: {}'.format(data.count()))

    print('----------------------------------------')
    print('\n')


def read_data(spark_session: SparkSession, file_path: str):
    # read song i94project file
    filename, file_extension = os.path.splitext(file_path)

    print(f'Loading file: {file_path} ...')
    start_time = time.time()
    # read SAS
    if file_extension == '.sas7bdat':
        data = spark_session.read \
            .format('com.github.saurfang.sas.spark') \
            .option('forceLowercaseNames', True) \
            .load(file_path)
    # read CSV
    elif file_extension == '.csv':
        data = spark_session.read.csv(file_path,
                                      header='true',
                                      inferSchema='true')
    else:
        raise ValueError(f'Not supported extension: {file_extension}!')

    print(f'Finished loading in {(time.time() - start_time):06.2f} sec.')

    # print data info
    print_data_info(data, file_path)

    return data


def read_dict(dict_file_path: str) -> []:
    """
        Reads dictionary key values (first column).

        :param dict_file_path: dictionary full path
        :return: list of key values for given dictionary
    """
    data = pd.read_csv(dict_file_path, skiprows=1, header=None)
    data = data[data.columns[0]].to_list()
    return data


def path_join(root: str, key: str) -> str:
    """ join path for s3 urls and local files """
    if root.startswith('s3'):
        return root + '/' + key
    else:
        return os.path.join(root, key)


def read_dicts(dict_file_dir: str) -> {}:
    """ read dictionaries of column valid values"""
    dict_column_valid_values = {}

    # country codes
    country_codes = read_dict(path_join(dict_file_dir, 'country_code.csv'))
    country_codes = [str(c) for c in country_codes]
    dict_column_valid_values['birth_country_code'] = country_codes
    dict_column_valid_values['residence_country_code'] = country_codes

    # us states
    us_states = read_dict(path_join(dict_file_dir, 'us_state.csv'))
    dict_column_valid_values['us_state_code'] = us_states

    # us ports
    us_ports = read_dict(path_join(dict_file_dir, 'us_port.csv'))
    dict_column_valid_values['us_port'] = us_ports

    # visa mode
    visa_modes = read_dict(path_join(dict_file_dir, 'visa_mode.csv'))
    dict_column_valid_values['visa_mode_id'] = visa_modes

    # visa type
    visa_types = read_dict(path_join(dict_file_dir, 'visa_type.csv'))
    dict_column_valid_values['visa_type_code'] = visa_types

    # arrival type
    arrival_types = read_dict(path_join(dict_file_dir, 'arrival_type.csv'))
    dict_column_valid_values['arrival_type_id'] = arrival_types

    return dict_column_valid_values


def rename_columns(data: DataFrame, new_column_names: []) -> DataFrame:
    """ rename dataframe columns """
    data = data.toDF(*new_column_names)
    return data


def map_columns(data: DataFrame, mapping: {}) -> DataFrame:
    """ rename and rearrange columns base on mapping dictionary"""
    select_expr = [F.col(c).alias(a) for c, a in mapping.items()]
    data = data.select(*select_expr)
    return data


def map_and_cast_columns(data: DataFrame, mapping: {}) -> DataFrame:
    """ rename and rearrange columns base on mapping dictionary"""
    select_expr = [F.col(c).cast(a[1]).alias(a[0]) for c, a in mapping.items()]
    data = data.select(*select_expr)
    return data


def cast_columns(data: DataFrame, column_names: [], type: str) -> DataFrame:
    for col in column_names:
        data = data.withColumn(col, data[col].cast(type))

    print_data_info(data, f'[DF] casted to {type} ', isDetailed=False)
    return data


def convert_columns(data: DataFrame, column_names: [], convert_func: F.udf) -> DataFrame:
    for col in column_names:
        data = data.withColumn(col, convert_func(col))

    print_data_info(data, f'[DF] converted {convert_func.__name__}', isDetailed=False)
    return data