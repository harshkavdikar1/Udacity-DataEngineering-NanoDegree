from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table = '',
                 redshift_conn_id = 'redshift',
                 create_sql_stmt = '',
                 drop_sql_stmt = '',
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_sql_stmt = create_sql_stmt
        self.drop_sql_stmt = drop_sql_stmt

    def execute(self, context):
        
        ## Postgre Hook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Drop table {}...".format(self.table))
        redshift_hook.run(self.drop_sql_stmt.format(self.table))

        self.log.info("Create table {}.".format(self.table))
        redshift_hook.run(self.create_sql_stmt)
