from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):
    # this means airflow will use context variable to fill this field
    template_fields = "s3_key"
    copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                IGNOREHEADER {}
                DELIMITER '{}'
               """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 *args,
                 **kwargs):
        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id,
        self.aws_credentials = aws_credentials,
        self.table = table,
        self.s3_bucket = s3_bucket,
        self.s3_key = s3_key,
        self.delimiter = delimiter,
        self.ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.postgres_conn_id)

        self.log(f"""Cleaning table {self.table} """)
        redshift.run(f"""DELETE FROM { self.table }""")
        self.log(f"""Copying data from S3 to Redshift""")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter
        )
        redshift.run(formatted_sql)


