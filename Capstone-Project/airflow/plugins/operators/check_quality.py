from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CheckQualityOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table = '',
                 redshift_conn_id = 'redshift',
                 test_stmt=None,
                 result=None,
                 *args, **kwargs):

        super(CheckQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.test_stmt = test_stmt
        self.result = result

    def execute(self, context):
        self.log.info("Check daya quality for table {}...".format(self.table))

        ## Postgre Hook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Fail: No results for {self.table}")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Fail: 0 rows in {self.table}")

        if self.test_stmt:
            output = redshift_hook.get_first(self.test_stmt)
            if self.result != output:
                raise ValueError(f"Fail: {output} != {self.result}")
        self.log.info(f"Success: {self.table} has {records[0][0]} records")