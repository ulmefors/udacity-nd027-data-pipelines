import logging


def no_results_validation(records, table=None):
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f'Error: Table {table} returns no results')
    num_records = records[0][0]
    if num_records < 1:
        raise ValueError(f'Error: Table {table} contains 0 records')
    logging.info(f'Passed: Table {table} contains {num_records} records')
    return True


class DataQualityTests:

    class DataQualityTest:

        def __init__(self, sql, validation, table=None):
            self.sql = sql
            self.validation = validation
            self.table = table
            self.records = None

        def validate(self):
            return self.validation(self.records, self.table)

    tests = [
        DataQualityTest(
            sql='SELECT * FROM users',
            validation=no_results_validation,
            table='users',
        ),
    ]
