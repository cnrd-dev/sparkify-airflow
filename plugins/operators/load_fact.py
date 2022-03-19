from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        table="",
        create_table_sql="",
        insert_data_into_table="",
        *args,
        **kwargs,
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table_sql = create_table_sql
        self.insert_data_into_table = insert_data_into_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Creating table '{self.table}'")
        redshift.run(self.create_table_sql)

        self.log.info("Inserting data into table '{self.table}'")
        redshift.run(self.insert_data_into_table)

        self.log.info("LoadFact completed")
