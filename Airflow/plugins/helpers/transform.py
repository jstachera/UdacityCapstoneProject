import os
import time

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# TODO: try/catch import for UI airflow no module found errors
#   Needed some investigation how to handle this cleaner.
try:
    from helpers.cleaning import Cleaner
    from helpers.common import read_data, print_data_info
except ImportError:
    from cleaning import Cleaner
    from common import read_data, print_data_info


class Transformer:
    _cleaner: Cleaner
    _spark: SparkSession

    def __init__(self, spark: SparkSession, dict_path: str):
        self._cleaner = Cleaner(dict_path)
        self._spark = spark

    def _read_data(self, path: str, dataset_name: str) -> DataFrame:
        print(f'Starting reading {dataset_name} dataset ...')
        timer = time.time()
        data = read_data(self._spark, path)
        print(f'Finished reading {dataset_name} dataset in: {(time.time() - timer):06.2f} sec.')
        return data

    def _write_data(self, data: DataFrame, path: str, dataset_name: str):
        print(f'Starting writing {dataset_name} dataset to: {path} ...')
        _, output_format = os.path.splitext(path)
        output_format = output_format.replace(".", "")
        timer = time.time()
        data. \
            coalesce(1). \
            write. \
            format(output_format). \
            save(path, mode='overwrite')
        print(f'Finished writing {dataset_name} dataset in: {(time.time() - timer):06.2f} sec.')

    def transform_immigration(self, immigration_input_path: str, immigration_output_path: str,
                              date_output_path: str) -> bool:

        # read immigration data
        imgDF = self._read_data(immigration_input_path, 'immigration')
        # transform
        print('Starting transform immigration dataset ...')
        timer = time.time()

        # clean demographic
        imgDF = self._cleaner.clean_immigration_data(imgDF)

        # extract dim dates
        self._extract_dim_date(imgDF, date_output_path)

        imgDF = imgDF.withColumn('stay_days', F.datediff(F.col('allowed_stay_date'), F.col('arrival_date')))

        print(f'Finished transform immigration dataset in: {(time.time() - timer):06.2f} sec.')

        print_data_info(imgDF, 'immigrationDF')

        # write
        self._write_data(imgDF, immigration_output_path, 'immigration')

        return True

    def _extract_dim_date(self, imgDF: DataFrame, output_path: str):
        print('Extracting dim_date from immigration data...')

        dateCols = [col for col in imgDF.columns if col.endswith('_date')]
        dates: DataFrame = None
        for dateCol in dateCols:
            dt = imgDF. \
                where(
                F.col(dateCol).isNotNull()
            ). \
                select(
                    F.col(dateCol).alias('date'),
                    F.year(F.col(dateCol)).cast('smallint').alias('year'),
                    F.quarter(F.col(dateCol)).cast('smallint').alias('quarter'),
                    F.month(F.col(dateCol)).cast('smallint').alias('month'),
                    F.dayofweek(F.col(dateCol)).cast('smallint').alias('day_of_week'),
                    F.dayofmonth(F.col(dateCol)).cast('smallint').alias('day_of_month'),
                    F.dayofyear(F.col(dateCol)).cast('smallint').alias('day_of_year'),
                    F.weekofyear(F.col(dateCol)).cast('smallint').alias('week_of_year')
            )
            if dates:
                dates = dates.union(dt).distinct()
            else:
                dates = dt

        print('Saving dim_date')
        self._write_data(dates, output_path, 'date')
        print('Finished saving dim_date')

        print('Finished extracting dim_date from immigration data.')

    def transform_us_state(self, demographics_input_path: str, us_state_dict_input_path: str, output_path: str) -> bool:
        # read demographics data
        demoDF = self._read_data(demographics_input_path, 'demographic')
        # transform
        print('Starting transform state dataset ...')
        timer = time.time()

        # clean demographic
        demoDF = self._cleaner.clean_demographic_data(demoDF)

        # calculate pct columns
        demoDF = demoDF \
            .withColumn('male_pct', demoDF['male_count'] / demoDF['total_population'] * 100) \
            .withColumn('female_pct', demoDF['female_count'] / demoDF['total_population'] * 100) \
            .withColumn('veteran_pct', demoDF['veteran_count'] / demoDF['total_population'] * 100) \
            .withColumn('foreigner_pct', demoDF['foreigner_count'] / demoDF['total_population'] * 100) \
            .withColumn('race_pct', demoDF['race_count'] / demoDF['total_population'] * 100)

        groupby_pivot_cols = ['us_state_code',
                              'us_state_name',
                              'median_age',
                              'male_count',
                              'male_pct',
                              'female_count',
                              'female_pct',
                              'total_population',
                              'veteran_count',
                              'veteran_pct',
                              'foreigner_count',
                              'foreigner_pct',
                              'avg_household_size']

        # pivot by race
        demoDF = demoDF.groupBy(*groupby_pivot_cols).pivot('race').sum('race_count', 'race_pct')

        # change race columns
        demoDF = demoDF.select('us_state_code',
                               'us_state_name',
                               'median_age',
                               'male_count',
                               'male_pct',
                               'female_count',
                               'female_pct',
                               'total_population',
                               'veteran_count',
                               'veteran_pct',
                               'foreigner_count',
                               'foreigner_pct',
                               'avg_household_size',
                               F.col('American Indian and Alaska Native_sum(race_count)').alias('indian_count'),
                               F.col('American Indian and Alaska Native_sum(race_pct)').alias('indian_pct'),
                               F.col('Asian_sum(race_count)').alias('asian_count'),
                               F.col('Asian_sum(race_pct)').alias('asian_pct'),
                               F.col('Black or African-American_sum(race_count)').alias('black_count'),
                               F.col('Black or African-American_sum(race_pct)').alias('black_pct'),
                               F.col('Hispanic or Latino_sum(race_count)').alias('hispanic_count'),
                               F.col('Hispanic or Latino_sum(race_pct)').alias('hispanic_pct'),
                               F.col('White_sum(race_count)').alias('white_count'),
                               F.col('White_sum(race_pct)').alias('white_pct'))

        avg_cols = ['median_age',
                    'male_pct',
                    'female_pct',
                    'avg_household_size',
                    'veteran_pct',
                    'foreigner_pct',
                    'indian_pct',
                    'asian_pct',
                    'black_pct',
                    'hispanic_pct',
                    'white_pct']

        sum_cols = ['male_count',
                    'female_count',
                    'total_population',
                    'veteran_count',
                    'foreigner_count',
                    'indian_count',
                    'asian_count',
                    'black_count',
                    'hispanic_count',
                    'white_count']

        pct_prec: int = 2
        avg_exprs_pct = [F.round(F.avg(x), pct_prec).cast('decimal(5,2)').alias(x) for x in avg_cols if
                         x.endswith('_pct')]
        avg_exprs_none_pct = [F.avg(x).cast('float').alias(x) for x in avg_cols if not x.endswith('_pct')]
        sum_exprs = [F.sum(x).cast('int').alias(x) for x in sum_cols]
        exprs = sum_exprs + avg_exprs_pct + avg_exprs_none_pct

        demoDF = demoDF. \
            groupBy('us_state_code', 'us_state_name'). \
            agg(*exprs). \
            orderBy('us_state_code')

        print_data_info(demoDF, 'us_state')

        print(f'Finished transform state dataset in: {(time.time() - timer):06.2f} sec.')
        # write
        self._write_data(demoDF, output_path, 'us_state')

        return True

    def transform_country(self, country_dict_input_path: str, temperature_input_path: str, output_path: str) -> bool:
        saved_columns = ['code', 'name', 'avg_temp_cels']

        # read country data
        countryDF = self._read_data(country_dict_input_path, 'country_dict')
        # read temperature data
        temperatureDF = self._read_data(temperature_input_path, 'temperature')
        # transform
        print('Starting transform country dataset ...')
        timer = time.time()
        # clean temp
        temperatureDF = self._cleaner.clean_temperature_data(temperatureDF)
        # join temp with country
        countryTempDF = countryDF. \
            join(temperatureDF, countryDF['name'] == temperatureDF['country'], how='left'). \
            select(*saved_columns)

        # cast country code from int to string type
        countryTempDF = countryTempDF.withColumn("code", F.col("code").cast(StringType()))

        print(f'Finished transform country dataset in: {(time.time() - timer):06.2f} sec.')
        # write
        self._write_data(countryTempDF, output_path, 'country')

        return True

    def transform_airports(self, input_path: str, output_path: str) -> bool:
        # read data
        data = self._read_data(input_path, 'airport')
        # transform
        print('Starting transform airport dataset ...')
        t_timer = time.time()
        data = self._cleaner.clean_airport_data(data)
        w_timer = time.time()
        print(f'Finished transform airport dataset in: {(time.time() - t_timer):06.2f} sec.')
        # write
        self._write_data(data, output_path, 'airport')

        return True
