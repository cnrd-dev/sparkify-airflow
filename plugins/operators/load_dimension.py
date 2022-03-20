from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"
    insert_sql = """
        INSERT INTO {} ({})
    """

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

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table_sql = create_table_sql
        self.insert_data_into_table = insert_data_into_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Creating table '{self.table}'")
        redshift.run(self.create_table_sql)

        self.log.info(f"Inserting data into table '{self.table}'")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.insert_data_into_table,
        )
        redshift.run(formatted_sql)

        self.log.info("LoadDim completed")
