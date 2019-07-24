from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.table = table,
        self.sql_stmt = sql_stmt,

    def execute(self, context):
        self.log.info(f""" Creating Postgres hook """)
        redshift = PostgresHook(postgress_conn_id=self.redshift_conn_id)
        self.log.info(f""" Loading Data into table {self.table} """)
        formatted_sql = self.sql_stmt.format(self.table, self.table)
        redshift.run(formatted_sql)
