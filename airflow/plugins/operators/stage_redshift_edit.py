from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 sql_stmt="",
                 json_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id,
        self.aws_credentials_id=aws_credentials_id,
        self.table=table,
        self.s3_bucket=s3_bucket,
        self.s3_key=s3_key,
        self.sql_stmt=sql_stmt,
        self.json_format=json_format

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        self.log.info(f""" Reading credentials """)
        credentials = aws_hook.get_credentials()
        self.log.info(f""" Creating Postgres hook """)
        redshift = PostgresHook(postgress_conn_id = self.redshift_conn_id)
        self.log.info(f"""Cleaning table {self.table} """)
        redshift.run(f"""DELETE FROM { self.table }""")
        self.log.info(f"""Copying data from S3 to Redshift""")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = self.sql_stmt.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_format
        )
        redshift.run(formatted_sql)
