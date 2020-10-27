from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_queries import SqlQueries

"""
    Stage to Redshift operator.
    Copies i94project from S3 specified location (bucket,key) to redshift table.
    By default, before the i94project is copied the destination table is truncated.
"""


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 deleteBeforeInsert=True,
                 copy_options=tuple(),
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.deleteBeforeInsert = deleteBeforeInsert
        self.aws_credentials_id = aws_credentials_id
        self.copy_options = copy_options

    def execute(self, context):
        self.log.info('StageToRedshiftOperator started execution')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        queries = SqlQueries()

        if self.deleteBeforeInsert:
            redshift.run(queries.get_truncate_statement(self.table))
            self.log.info('Truncated table: {}'.format(self.table))

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info("> Copying from S3 to Redshift: {}".format(s3_path))
        copy_options = '\n\t\t\t'.join(self.copy_options)
        self.log.info(">> Copy options: {}".format(copy_options))
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            copy_options
        )
        self.log.info(">> Full copy statement: {}".format(formatted_sql))
        redshift.run(formatted_sql)
        self.log.info('StageToRedshiftOperator finished execution')
