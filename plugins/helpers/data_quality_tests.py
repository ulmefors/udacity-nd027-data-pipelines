import logging


class DataQualityTest:
    """Test case for data quality"""

    def __init__(self, sql, validation, table=None):
        """Initialize test case

        args:
            sql: SQL statement to execute
            validation: Validation method for query results
            table: Table name
        """
        self.sql = sql
        self.validation = validation
        self.table = table
        self.records = None

    def validate(self):
        return self.validation(self.records, self.table)

    @staticmethod
    def no_results_validation(records, table=None):
        """Validate non-empty table

        args:
            records: Query results
            table: Table name
        returns:
            True if test passed
        """
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f'Error: Table {table} returns no results')
        n_records = records[0][0]
        if n_records < 1:
            raise ValueError(f'Error: Table {table} contains 0 records')
        logging.info(f'Passed: Table {table} contains {n_records} records')
        return True

    @staticmethod
    def no_results_test(table):
        """Create test case to verify that table is not empty

        args:
            table: Table name

        returns:
            DataQualityTest
        """
        return DataQualityTest(
            sql=f'SELECT count(*) FROM {table}',
            validation=DataQualityTest.no_results_validation,
            table=table,
        )
