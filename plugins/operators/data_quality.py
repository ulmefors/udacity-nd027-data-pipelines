import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.tables = tables

    def execute(self, context):
        postgres = PostgresHook(self.postgres_conn_id)

        # Verify that tables aren't empty
        for table in self.tables:
            records = postgres.get_records(f'SELECT COUNT(*) FROM {table}')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Error: Table {table} returns no results')
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f'Error: Table {table} contains 0 records')
            logging.info(f'Passed: Table {table} contains {num_records} records')
