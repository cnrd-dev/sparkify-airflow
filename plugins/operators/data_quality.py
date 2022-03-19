from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        tables_and_columns={
            "songplays": ["playid", "artistid"],
            "users": ["userid"],
            "songs": ["songid", "artistid"],
            "artists": ["artistid"],
            "time": ["start_time"],
        },
        *args,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_and_columns = tables_and_columns

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Test for rows > 0
        for table in self.tables_and_columns:
            rows = redshift.get_records(SqlQueries.test_count_rows.format(table))
            if len(rows) > 1 or len(rows[0]) > 1:
                self.log.info(f"Test passed. '{table}' has {rows[0][0]}.")
            else:
                raise ValueError(f"Test failed. '{table}' has 0 rows.")

        # Test for nulls
        for table, columns in self.tables_and_columns.items():
            for column in columns:
                rows = redshift.get_records(SqlQueries.test_for_nulls.format(table, column))
                if len(rows) > 1 or len(rows[0]) > 1:
                    raise ValueError(f"Test failed. '{table}' on column '{column}' has {rows[0][0]} NULL rows.")
                else:
                    self.log.info(f"Test passed. '{table}' on column '{column}' has no NULLs.")

        self.log.info("DataQaulity check completed")
