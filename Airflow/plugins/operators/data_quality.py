from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
    Data quality operator.
    Executes quality checks passed in the constructor.
"""


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('DataQualityOperator has started')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        error_count = 0
        failing_tests = []

        print('Testing {} checks.'.format(len(self.dq_checks)))

        for name, check in self.dq_checks.items():
            sql = check.get('sql')
            exp_result = check.get('expected_result')
            records = redshift_hook.get_records(sql)[0]
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
            else:
                print(f"Data quality check '{name}' passed.")

        if error_count > 0:
            self.log.info('DataQualityOperator failed!')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed!')

        self.log.info('DataQualityOperator has finished')
