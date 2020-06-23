from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CopyTableOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table ='',
                 schema ='public',
                 s3_bucket = '',
                 s3_load_prefix = '',
                 csv_file_name = None,
                 delimiter = ',',
                 aws_conn_id = 'aws_credentials',
                 redshift_conn_id = 'redshift',
                 iam_role = '',
                 *args, **kwargs):

        super(CopyTableOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.schema = schema
        self.s3_bucket = s3_bucket
        self.s3_load_prefix = s3_load_prefix
        self.csv_file_name = csv_file_name
        self.delimiter = delimiter
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.iam_role = iam_role


    def execute(self, context):
        self.log.info("Start loading dimensional table from .sas file...")

        ## Postgre Hook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # AWS Hook
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()



        copy_csv = """
        COPY {}.{}
        FROM 's3://{}/{}/{}'
        CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
        IGNOREHEADER 1
        CSV
        DELIMITER '{}'
        ;
        """
        copy_parquet = """
        COPY {}.{}
        FROM 's3://{}/{}/'
        IAM_ROLE '{}'
        FORMAT AS PARQUET;
        """

        copy_parquet_sql = copy_parquet.format(self.schema,
                                               self.table,
                                               self.s3_bucket,
                                               self.s3_load_prefix,
                                               self.iam_role)

        copy_csv_sql = copy_csv.format(self.schema,
                               self.table,
                               self.s3_bucket, 
                               self.s3_load_prefix, 
                               self.csv_file_name,
                               credentials.access_key,
                               credentials.secret_key,
                               self.delimiter)

        self.log.info("Copy table {}.".format(self.table))
        if self.csv_file_name: 
            redshift_hook.run(copy_csv_sql)
        else: 
            redshift_hook.run(copy_parquet_sql)
        self.log.info("Finished copy table {}.".format(self.table))
