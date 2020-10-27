import datetime
import os
from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator

from operators import StageToRedshiftOperator, DataQualityOperator
from helpers import SqlChecks

"""
    Set AWS environment variables.
    Required for importing dicts from S3 bucket by pandas lib.
"""
os.environ['AWS_ACCESS_KEY_ID'] = ''
os.environ['AWS_SECRET_ACCESS_KEY'] = ''

"""
    Set spark packages.
    Required by SparkSubmit operator.
"""
spark_packages = 'org.apache.hadoop:hadoop-aws:3.2.0,' \
                'org.apache.hadoop:hadoop-common:3.2.0,' \
                'com.amazonaws:aws-java-sdk-bom:1.11.375,' \
                'saurfang:spark-sas7bdat:3.0.0-s_2.12'

"""
    Immigration ETL dag.
    Configuration:
        - dag does not have dependencies on past runs
        - on failure, the task are retried 3 times
        - retries happen every 5 minutes
        - catchup is turned off
        - do not email on retrys
"""

"""
    DAG initialization
"""
default_args = {
    'owner': 'jstachera',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'start_date': datetime.datetime(2016, 4, 1, 0, 0, 0, 0),
    'end_date': datetime.datetime(2016, 4, 30, 0, 0, 0, 0),
    'max_active_runs': 1
}

dag = DAG('immigration_dag',
          default_args=default_args,
          description='ETL process for I94 immigration i94project',
          schedule_interval='@monthly')

#    Operator definition

start_operator = DummyOperator(task_id='start_execution', dag=dag)

# load dim_arrival_types table
load_dim_arrival_type = StageToRedshiftOperator(
    task_id='load_dim_arrival_type',
    dag=dag,
    table="dim_arrival_type",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="i94project",
    s3_key="dicts/arrival_type.csv",
    copy_options=("CSV", "REGION 'us-west-2'", "IGNOREHEADER 1")
)

# load dim_visa_mode table
load_dim_visa_mode = StageToRedshiftOperator(
    task_id='load_dim_visa_mode',
    dag=dag,
    table="dim_visa_mode",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="i94project",
    s3_key="dicts/visa_mode.csv",
    copy_options=("CSV", "REGION 'us-west-2'", "IGNOREHEADER 1")
)

# load dim_visa_type table
load_dim_visa_type = StageToRedshiftOperator(
    task_id='load_dim_visa_type',
    dag=dag,
    table="dim_visa_type",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="i94project",
    s3_key="dicts/visa_type.csv",
    copy_options=("CSV", "REGION 'us-west-2'", "IGNOREHEADER 1")
)

# process us airport in spark
process_dim_us_airport = SparkSubmitOperator(
    application=r"/usr/local/airflow/plugins/helpers/spark_dispatch.py",
    application_args=[r"process_airport",  # command
                      r"s3a://i94project/dicts",  # dictionaries
                      r"s3a://i94project/stage/input/airport-codes.csv",  # input_path
                      r"s3a://i94project/stage/output/airport.parquet"],  # output_path
    task_id="process_dim_us_airport",
    packages=spark_packages,
    dag=dag
)

# load dim_us_airport table
load_dim_us_airport = StageToRedshiftOperator(
    task_id='load_dim_us_airport',
    dag=dag,
    table="dim_us_airport",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="i94project",
    s3_key="stage/output/airport.parquet",
    # copy_options=("CSV", "REGION 'us-west-2'", "IGNOREHEADER 1")
    copy_options=('FORMAT AS PARQUET',)
)

# process country in spark
process_dim_country = SparkSubmitOperator(
    application=r"/usr/local/airflow/plugins/helpers/spark_dispatch.py",
    application_args=[r"process_country",  # command
                      r"s3a://i94project/dicts",  # dictionaries
                      r"s3a://i94project/dicts/country_code.csv",  # input_path for country dict
                      r"s3a://i94project/stage/input/GlobalLandTemperaturesByCountry.csv",
                      # input_path for GlobalLandTemperaturesByCountry
                      r"s3a://i94project/stage/output/country.parquet"],  # output_path
    task_id="process_dim_country",
    packages=spark_packages,
    dag=dag
)

# load dim_country table
load_dim_country = StageToRedshiftOperator(
    task_id='load_dim_country',
    dag=dag,
    table="dim_country",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="i94project",
    s3_key="stage/output/country.parquet",
    # copy_options=("CSV", "REGION 'us-west-2'", "IGNOREHEADER 1")
    copy_options=('FORMAT AS PARQUET',)
)

# process us_state in spark
process_dim_us_state = SparkSubmitOperator(
    application=r"/usr/local/airflow/plugins/helpers/spark_dispatch.py",
    application_args=[r"process_us_state",  # command
                      r"s3a://i94project/dicts",  # dictionaries
                      r"s3a://i94project/stage/input/us-cities-demographics.csv",
                      # input_path for us-cities-demographics
                      r"s3a://i94project/dicts/us_state.csv",  # input_path for us state dict
                      r"s3a://i94project/stage/output/us_state.parquet"],  # output_path
    task_id="process_dim_us_state",
    packages=spark_packages,
    dag=dag
)

# load dim_us_state table
load_dim_us_state = StageToRedshiftOperator(
    task_id='load_dim_us_state',
    dag=dag,
    table="dim_us_state",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="i94project",
    s3_key="stage/output/us_state.parquet",
    # copy_options=("CSV", "REGION 'us-west-2'", "IGNOREHEADER 1")
    copy_options=('FORMAT AS PARQUET',)
)

# process us_state in spark
process_fact_immigration = SparkSubmitOperator(
    application=r"/usr/local/airflow/plugins/helpers/spark_dispatch.py",
    application_args=[r"process_immigration",  # command
                      r"s3a://i94project/dicts",  # dictionaries
                      "s3a://i94project/stage/input/i94_{{ execution_date.strftime('%b%y').lower() }}_sub.csv",
                      r"s3a://i94project/stage/output/immigration.parquet",  # output path for immigration
                      r"s3a://i94project/stage/output/date.parquet"],  # output path for date
    task_id="process_fact_immigration",
    packages=spark_packages,
    dag=dag
)

# load dim_immigration table
load_fact_immigration = StageToRedshiftOperator(
    task_id='load_fact_immigration',
    dag=dag,
    table="fact_immigration",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="i94project",
    s3_key="stage/output/immigration.parquet",
    deleteBeforeInsert=False,
    # copy_options=("CSV", "REGION 'us-west-2'", "IGNOREHEADER 1")
    copy_options=('FORMAT AS PARQUET',)
)

# load dim_date table
load_dim_date = StageToRedshiftOperator(
    task_id='load_dim_date',
    dag=dag,
    table="dim_date",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="i94project",
    s3_key="stage/output/date.parquet",
    # copy_options=("CSV", "REGION 'us-west-2'", "IGNOREHEADER 1")
    copy_options=('FORMAT AS PARQUET',)
)

checks = SqlChecks()
run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=checks.load_dq_checks()
)

end_operator = DummyOperator(task_id='end_execution', dag=dag)

# task dependencies
start_operator >> [load_dim_arrival_type,
                   load_dim_visa_mode,
                   load_dim_visa_type,
                   process_fact_immigration,
                   process_dim_us_state,
                   process_dim_us_airport,
                   process_dim_country]

process_fact_immigration >> [load_dim_date,
                             load_fact_immigration]

process_dim_us_state >> load_dim_us_state

process_dim_us_airport >> load_dim_us_airport

process_dim_country >> load_dim_country

[load_dim_arrival_type,
 load_dim_visa_mode,
 load_dim_visa_type,
 load_dim_date,
 load_fact_immigration,
 load_dim_us_state,
 load_dim_us_airport,
 load_dim_country] >> run_quality_checks

run_quality_checks >> end_operator