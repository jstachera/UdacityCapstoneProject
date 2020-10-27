import os
import json

default_dq_checks = (
    {
        "dim_country": {
            "sql": "select (case when exists(select * from public.dim_country limit 1) then 1 else 0 end)",
            "expected_result": 1
        },
        "dim_date": {
            "sql": "select (case when exists(select * from public.dim_date limit 1) then 1 else 0 end)",
            "expected_result": 1
        },
        "dim_arrival_type": {
            "sql": "select (case when exists(select * from public.dim_arrival_type limit 1) then 1 else 0 end)",
            "expected_result": 1
        },
        "dim_visa_mode": {
            "sql": "select (case when exists(select * from public.dim_visa_mode limit 1) then 1 else 0 end)",
            "expected_result": 1
        },
        "dim_visa_type": {
            "sql": "select (case when exists(select * from public.dim_visa_type limit 1) then 1 else 0 end)",
            "expected_result": 1
        },
        "dim_us_airport": {
            "sql": "select (case when exists(select * from public.dim_us_airport limit 1) then 1 else 0 end)",
            "expected_result": 1
        },
        "dim_us_state": {
            "sql": "select (case when exists(select * from public.dim_us_state limit 1) then 1 else 0 end)",
            "expected_result": 1
        },
        "fact_immigration": {
            "sql": "select (case when exists(select * from public.fact_immigration limit 1) then 1 else 0 end)",
            "expected_result": 1
        }
    })

"""
    Data quality checks.
"""


class SqlChecks:
    path: str = './plugins/helpers/data_quality_checks.json';

    def load_dq_checks(self) -> {}:
        """ Load data quality checks"""
        try:
            dqc = None
            print('Current dir: {}'.format(os.getcwd()))
            with open(self.path) as f:
                dqc = json.load(f)
            print('Loaded {} data quality checks.'.format(len(dqc)))
        except Exception as ex:
            print(f'Error loading dq_checks! {str(ex)}')
            print('Returning the defaults.')
            dqc = default_dq_checks

        return dqc
