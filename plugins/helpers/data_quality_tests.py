import logging


class DataQualityTest:

    def __init__(self, sql, validation, table=None):
        self.sql = sql
        self.validation = validation
        self.table = table
        self.records = None

    def validate(self):
        return self.validation(self.records, self.table)

    @staticmethod
    def no_results_validation(records, table=None):
        n_records = len(records)
        if n_records < 1 or len(records[0]) < 1:
            raise ValueError(f'Error: Table {table} returns no results')
        logging.info(f'Passed: Table {table} contains {n_records} records')
        return True

    @staticmethod
    def no_results_test(table):
        return DataQualityTest(
            sql=f'SELECT * FROM {table}',
            validation=DataQualityTest.no_results_validation,
            table=table,
        )
