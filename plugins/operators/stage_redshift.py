from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS {}
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        file_format="",
        *args,
        **kwargs,
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Copying data from S3 to Redshift")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.file_format,
        )
        redshift.run(formatted_sql)

        # Test that data was loaded successfully
        rows = redshift.get_first(SqlQueries.test_count_rows.format(self.table))

        if rows[0] > 1:
            self.log.info(f"Test passed. '{self.table}' has {rows[0]}.")
        else:
            raise ValueError(f"Test failed. '{self.table}' has 0 rows.")

        self.log.info("StageToRedshift completed")
