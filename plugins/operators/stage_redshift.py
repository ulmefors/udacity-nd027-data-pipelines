from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM '{}'
        IAM_ROLE '{}'
        JSON 'auto' REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 iam_role_arn='',
                 region='',
                 delete=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.iam_role_arn = iam_role_arn
        self.region = region
        self.delete = delete

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete:
            self.log.info(f'Delete data from Redshift table {self.table}')
            redshift.run(f'DELETE FROM {self.table}')

        self.log.info('Copy data from S3 to Redshift')
        rendered_s3_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_s3_key}'
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.iam_role_arn,
            self.region,
        )
        redshift.run(formatted_sql)
