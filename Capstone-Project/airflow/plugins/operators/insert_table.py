from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class InsertTableOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table ='',
                 schema ='public',
                 insert_sql_stmt ='',
                 redshift_conn_id = 'redshift',
                 *args, **kwargs):

        super(InsertTableOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.schema = schema
        self.insert_sql_stmt = insert_sql_stmt
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        self.log.info("Start insert table ...")

        ## Postgre Hook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        insert_sql = "INSERT INTO {} ({})".format(self.table, self.insert_sql_stmt)
        self.log.info("Insert table {}.".format(self.table))
        redshift_hook.run(insert_sql)
        self.log.info("Finished insert table {}.".format(self.table))