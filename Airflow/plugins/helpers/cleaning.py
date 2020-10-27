import time
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf

# TODO: try/catch import for UI airflow no module found errors
#   Needed some investigation how to handle this cleaner.
try:
    from helpers.common import \
        read_dicts, \
        map_columns, \
        print_data_info, \
        cast_columns, \
        convert_columns
except ImportError:
    from common import \
        read_dicts, \
        map_columns, \
        print_data_info, \
        cast_columns, \
        convert_columns


class Cleaner:
    _dict_path: str

    def __init__(self, dict_path: str):
        self._dict_path = dict_path

    def convert_unixts_to_datetime(unix_ts):
        try:
            if unix_ts is not None:
                start = datetime(1960, 1, 1)
                return start + timedelta(days=int(unix_ts))
            else:
                return None
        except:
            return None

    def _read_dict_column_valid_values(self) -> {}:
        print('\tReading dicts ...')
        dicts = read_dicts(self._dict_path)
        print('\tFinished reading dicts')
        return dicts

    def _dict_validation(self, data: DataFrame, verbose: bool = False) -> DataFrame:

        """
            Clean i94project by column.
            Remove rows which doest not belong to list of values.

            :param data: spark i94project frame to be filtered
            :param dictValid: dictionary of (column_name, values)
            :param verbose: flag if verbose mode
            :return: filtered spark frame
        """

        dict_column_valid_values = self._read_dict_column_valid_values()

        if not dict_column_valid_values:
            if verbose:
                print(f'\t| No columns to clean.')

        print(f'\t| Row count: {data.count()} before cleaning')
        for column, values in dict_column_valid_values.items():
            if column in data.columns:
                data = data.filter(data[column].isin(values))
                if verbose:
                    print(f'\t| Row count: {data.count()}, after cleaning column: {column}')

        return data

    def clean_immigration_data(self, imgDF: DataFrame):
        """ Demographic final column names """
        column_mapping = {
            'CICID': 'id',
            'I94YR': 'report_year',
            'I94MON': 'report_month',
            'I94CIT': 'birth_country_code',
            'I94RES': 'residence_country_code',
            'GENDER': 'gender',
            'BIRYEAR': 'birth_year',
            'I94BIR': 'person_age',
            'ADMNUM': 'admission_number',
            'DTADFILE': 'added_date',
            'I94PORT': 'us_airport_code',
            'I94ADDR': 'us_state_code',
            'ARRDATE': 'arrival_date',
            'I94MODE': 'arrival_type_id',
            'I94VISA': 'visa_mode_id',
            'VISATYPE': 'visa_type_code',
            'AIRLINE': 'airline_code',
            'FLTNO': 'flight_number',
            'DTADDTO': 'allowed_stay_date',
            # '  ':	    'stay_days', # computed
            'DEPDATE': 'departure_date'
        }

        im_column_to_int = ['id',
                            'birth_country_code',
                            'residence_country_code',
                            'arrival_date',
                            'departure_date',
                            'added_date']

        im_column_to_smallint = [
            'report_year',
            'report_month',
            'birth_year',
            'person_age',
            'arrival_type_id',
            'visa_mode_id'
        ]

        im_column_to_long = ['admission_number']

        im_column_to_str = [
            'birth_country_code',
            'residence_country_code'
        ]

        """
            Immigration columns converted date type
        """
        im_column_convered_to_date = ['arrival_date', 'departure_date']

        # rename/reorder columns
        imgDF = map_columns(imgDF, column_mapping)
        imgDF = cast_columns(imgDF, im_column_to_int, 'int')
        imgDF = cast_columns(imgDF, im_column_to_smallint, 'smallint')
        imgDF = cast_columns(imgDF, im_column_to_long, 'long')
        imgDF = convert_columns(imgDF, im_column_convered_to_date,
                                udf(lambda x: datetime(1960, 1, 1) + timedelta(days=int(x)) if x is not None else None,
                                    T.DateType()))
        imgDF = imgDF.withColumn('added_date', F.to_date(F.col('added_date').cast('string'), 'yyyyMMdd'))
        imgDF = imgDF.withColumn('allowed_stay_date', F.to_date(F.col('allowed_stay_date').cast('string'), 'MMddyyyy'))
        imgDF = cast_columns(imgDF, im_column_to_str, 'string')
        print_data_info(imgDF, isDetailed=True)

        imgDF = self._dict_validation(imgDF, True)

        return imgDF

    def clean_demographic_data(self, demoDF: DataFrame):

        """ Demographic final column names """
        column_mapping = {
            'City': 'city',
            'State Code': 'us_state_code',
            'State': 'us_state_name',
            'Median Age': 'median_age',
            'Male Population': 'male_count',
            'Female Population': 'female_count',
            'Total Population': 'total_population',
            'Number of Veterans': 'veteran_count',
            'Foreign-born': 'foreigner_count',
            'Average Household Size': 'avg_household_size',
            'Race': 'race',
            'Count': 'race_count'
        }

        # rename columns and sort by state and city
        demoDF = map_columns(demoDF, column_mapping)

        print_data_info(demoDF, isDetailed=True)

        # cast _count columns to int
        # demoDF = cast_columns(demoDF, [col for col in demoDF.columns if col.endswith('_count')], 'int')

        # cast specific columns
        demoDF = demoDF \
            .withColumn('median_age', F.col('median_age').cast('float')) \
            .withColumn('race_count', F.col('race_count').cast('long'))

        # upper the code primary colum
        demoDF = demoDF.withColumn('us_state_code', F.upper(F.col('us_state_code')))

        return demoDF

    def clean_temperature_data(self, tempDF: DataFrame):
        """
            Clean temperature data.
        :param tempDF: temperature spark data frame
        :return: cleaned: temperature data set
        """

        country_to_remove = [r"DENMARK", r"FRANCE", r"NETHERLANDS", r"UNITED KINGDOM"]

        country_to_update = {
            r"AMERICAN SAMOA": r"WESTERN SAMOA",
            r"BONAIRE, SAINT EUSTATIUS AND SABA": r"CARIBBEAN NETHERLANDS",
            r"CÔTE D'IVOIRE": r"IVORY COAST",
            r"CONGO (DEMOCRATIC REPUBLIC OF THE)": r"CONGO",
            r"CURAÇAO": r"CURACAO",
            r"DENMARK (EUROPE)": r"DENMARK",
            r"FRANCE (EUROPE)": r"FRANCE",
            r"NETHERLANDS (EUROPE)": r"NETHERLANDS",
            r"SAINT BARTHÃ©LEMY": r"SAINT BARTHELEMY",
            r"UNITED KINGDOM (EUROPE)": r"UNITED KINGDOM",
            r"FALKLAND ISLANDS (ISLAS MALVINAS)": r"FALKLAND ISLANDS"
        }

        timer = time.time()
        print(f'\tStarting cleaning temperature dataset ...')

        # calculate average temperature
        data = tempDF. \
            where(F.col('AverageTemperature').isNotNull()). \
            groupBy("Country"). \
            avg('AverageTemperature'). \
            withColumnRenamed("AVG(AverageTemperature)", "avg_temp_cels")

        # uppercase country column
        data = data.withColumn('upper_country', F.upper('Country')).drop('Country').withColumnRenamed("upper_country",
                                                                                                      "country")

        # outliers
        #   -Denmark (Europe)	7.832859444 < correct one
        #   -Denmark	-18.0530507
        # remove all countries that have corresponding (Europe) pair
        data = data.filter(~data['country'].isin(country_to_remove))

        # rename country names
        data = data.na.replace(country_to_update, 1, "Country")

        print(f'\tFinished cleaning temperature dataset in: {(time.time() - timer):06.2f} sec.')

        return data

    def clean_airport_data(self, data: DataFrame) -> DataFrame:
        """
            Clean airport i94project:
                - filter by US country
                - remove null rows in local_code (primary key)
                - extracts us state code from iso_region and do uppercase
                - cast elevation to float

            :param data: spark airport dataframe
            :return: cleaned spark airport dataframe
        """

        timer = time.time()
        print(f'\tStarting cleaning airport dataset ...')

        # columns to save in following order
        save_columns = ['local_code', 'type', 'name', 'elevation_ft', 'us_state_code', 'municipality', 'coordinates']

        # Filter:
        #   - only us country
        #   - not null: 'local code', 'name', 'iso region'
        #   - drop duplicates for 'local code' (primary key in final dataset)
        data = data.where(
            (F.col('iso_country') == 'US') &
            F.col('local_code').isNotNull() &
            (F.length(F.col('local_code')) <= 5) &
            F.col('name').isNotNull() &
            F.col('iso_region').isNotNull()) \
            .dropDuplicates(subset=['local_code'])

        # Clean:
        #   - extract us state code
        #   - cast elevation to float
        #   - select only relevant columns
        data = data \
            .withColumn("us_state_code", F.upper(F.substring(F.col("iso_region"), 4, 2))) \
            .withColumn("elevation_ft", F.col("elevation_ft").cast("float")) \
            .select(*save_columns)

        # Validate:
        #   - validated us_state_code column
        data = self._dict_validation(data, True)

        print(f'\tFinished cleaning airport dataset in: {(time.time() - timer):06.2f} sec.')

        return data
