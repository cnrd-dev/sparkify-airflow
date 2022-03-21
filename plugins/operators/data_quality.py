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
        dq_checks="",
        *args,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.dq_checks:
            sql = check.get("check_sql")
            exp_result = check.get("expected_result")
            type_result = check.get("type")

            rows_count = redshift.get_first(sql)[0]

            # compare with the expected results
            if type_result == "greater":
                if rows_count > exp_result:
                    self.log.info(f"Test passed. Table with {rows_count} rows is greater than {exp_result}.")
                else:
                    raise ValueError(f"Test failed. Table with {rows_count} rows is not greater than {exp_result}.")
            elif type_result == "equal":
                if rows_count == exp_result:
                    self.log.info(f"Test passed. Table with {rows_count} rows is equal to {exp_result}.")
                else:
                    raise ValueError(f"Test failed. Table with {rows_count} rows is equal to {exp_result}.")

        self.log.info("DataQuality check completed")
