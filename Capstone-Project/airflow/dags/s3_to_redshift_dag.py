from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable

from airflow.operators import (ExtractionFromSASOperator, CreateTableOperator, CopyTableOperator, CheckQualityOperator, InsertTableOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'weinanli',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': 300,
    'email_on_retry': False
}

dag = DAG('S3_to_redshift_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False)

extract_sas_data_operator = ExtractionFromSASOperator(
	task_id ='Extract_data_from_SAS_save_as_csv_in_s3bucket',
	dag=dag,
	s3_bucket = 'uda-capstone-data',
  s3_load_prefix = 'csv_data',
  s3_save_prefix = 'csv_data',
  file_name = 'I94_SAS_Labels_Descriptions.SAS')



create_immigration_table = CreateTableOperator(
  task_id = 'Create_immigration_table',
  dag=dag,
  table = 'immigration',
  create_sql_stmt = SqlQueries.immigrant_table_create,
  drop_sql_stmt = SqlQueries.drop_table
  )

load_immigration_table = CopyTableOperator(
  task_id = 'Load_immigration_table',
  dag=dag,
  table = 'immigration',
  schema ='public',
  s3_bucket = 'uda-capstone-data',
  s3_load_prefix = 'sas_data',
  iam_role = Variable.get("IAM_ROLE")
  )

data_quality_check_on_immigration = CheckQualityOperator(
        task_id="Check_data_quality_on_immigration",
        dag=dag,
        table='immigration'
    )

create_i94cit_i94res_table = CreateTableOperator(
  task_id = 'Create_i94cit_i94res_table',
  dag=dag,
  table = 'i94cit_i94res',
  create_sql_stmt = SqlQueries.i94cit_i94res_table_create,
  drop_sql_stmt = SqlQueries.drop_table
  )

load_i94cit_i94res_table = CopyTableOperator(
  task_id = 'Load_i94cit_i94res_table',
  dag=dag,
  table = 'i94cit_i94res',
  schema ='public',
  s3_bucket = 'uda-capstone-data',
  s3_load_prefix = 'csv_data',
  csv_file_name = 'i94cit_i94res.csv'
  )

data_quality_check_on_i94cit_i94res= CheckQualityOperator(
        task_id="Check_data_quality_on_i94cit_i94res",
        dag=dag,
        table='i94cit_i94res'
    )

create_i94mode_table = CreateTableOperator(
  task_id = 'Create_i94mode_table',
  dag=dag,
  table = 'i94mode',
  create_sql_stmt = SqlQueries.i94mode_table_create,
  drop_sql_stmt = SqlQueries.drop_table
  )

load_i94mode_table = CopyTableOperator(
  task_id = 'Load_i94mode_table',
  dag=dag,
  table = 'i94mode',
  schema ='public',
  s3_bucket = 'uda-capstone-data',
  s3_load_prefix = 'csv_data',
  csv_file_name = 'i94mode.csv'
  )

data_quality_check_on_i94mode= CheckQualityOperator(
        task_id="Check_data_quality_on_i94mode",
        dag=dag,
        table='i94mode'
    )


create_i94addr_table = CreateTableOperator(
  task_id = 'Create_i94addr_table',
  dag=dag,
  table = 'i94addr',
  create_sql_stmt = SqlQueries.i94addr_table_create,
  drop_sql_stmt = SqlQueries.drop_table
)

load_i94addr_table = CopyTableOperator(
  task_id = 'Load_i94addr_table',
  dag=dag,
  table = 'i94addr',
  schema ='public',
  s3_bucket = 'uda-capstone-data',
  s3_load_prefix = 'csv_data',
  csv_file_name = 'i94addr.csv'
  )

data_quality_check_on_i94addr= CheckQualityOperator(
        task_id="Check_data_quality_on_i94addr",
        dag=dag,
        table='i94addr'
    )

create_i94visa_table = CreateTableOperator(
  task_id = 'Create_i94visa_table',
  dag=dag,
  table = 'i94visa',
  create_sql_stmt = SqlQueries.i94visa_table_create,
  drop_sql_stmt = SqlQueries.drop_table
  )



load_i94visa_table = CopyTableOperator(
  task_id = 'Load_i94visa_table',
  dag=dag,
  table = 'i94visa',
  schema ='public',
  s3_bucket = 'uda-capstone-data',
  s3_load_prefix = 'csv_data',
  csv_file_name = 'i94visa.csv'
  )

data_quality_check_on_i94visa= CheckQualityOperator(
        task_id="Check_data_quality_on_i94visa",
        dag=dag,
        table='i94visa'
    )


create_i94port_table = CreateTableOperator(
  task_id = 'Create_i94port_table',
  dag=dag,
  table = 'i94port',
  create_sql_stmt = SqlQueries.i94port_table_create,
  drop_sql_stmt = SqlQueries.drop_table
  )

load_i94port_table = CopyTableOperator(
  task_id = 'Load_i94port_table',
  dag=dag,
  table = 'i94port',
  schema ='public',
  s3_bucket = 'uda-capstone-data',
  s3_load_prefix = 'csv_data',
  csv_file_name = 'i94port.csv'
  )

data_quality_check_on_i94port= CheckQualityOperator(
        task_id="Check_data_quality_on_i94port",
        dag=dag,
        table='i94port'
    )

create_us_cities_demographics_table = CreateTableOperator(
  task_id = 'Create_us_cities_demographics_table',
  dag=dag,
  table = 'us_cities_demographics',
  create_sql_stmt = SqlQueries.us_cities_demographics_table_create,
  drop_sql_stmt = SqlQueries.drop_table
  )

load_us_cities_demographics_table = CopyTableOperator(
  task_id = 'Load_us_cities_demographics_table',
  dag=dag,
  table = 'us_cities_demographics',
  schema ='public',
  s3_bucket = 'uda-capstone-data',
  s3_load_prefix = 'csv_data',
  csv_file_name = 'us-cities-demographics.csv',
  delimiter = ';'
  )

data_quality_check_on_us_cities_demographics= CheckQualityOperator(
        task_id="Check_data_quality_on_us_cities_demographics",
        dag=dag,
        table='us_cities_demographics'
    )

create_us_state_race_table = CreateTableOperator(
  task_id = 'Create_us_state_race_table',
  dag=dag,
  table = 'us_state_race',
  create_sql_stmt = SqlQueries.us_state_race_table_create,
  drop_sql_stmt = SqlQueries.drop_table
 
  )
load_us_state_race_table = InsertTableOperator(
  task_id = 'Load_us_state_race_table',
  dag=dag,
  table = 'us_state_race',
  insert_sql_stmt = SqlQueries.us_state_race_table_insert
  )

data_quality_check_on_us_state_race= CheckQualityOperator(
        task_id="Check_data_quality_on_us_state_race",
        dag=dag,
        table='us_state_race'
    )

create_us_cities_table = CreateTableOperator(
  task_id = 'Create_us_cities_table',
  dag=dag,
  table = 'us_cities',
  create_sql_stmt = SqlQueries.us_cities_table_create,
  drop_sql_stmt = SqlQueries.drop_table
 
  )
load_us_cities_table = InsertTableOperator(
  task_id = 'Load_us_cities_table',
  dag=dag,
  table = 'us_cities',
  insert_sql_stmt = SqlQueries.us_cities_table_insert
  )

data_quality_check_on_us_cities= CheckQualityOperator(
        task_id="Check_data_quality_on_us_cities",
        dag=dag,
        table='us_cities'
    )

create_airport_table = CreateTableOperator(
  task_id = 'Create_airport_table',
  dag=dag,
  table = 'airport',
  create_sql_stmt = SqlQueries.airport_table_create,
  drop_sql_stmt = SqlQueries.drop_table
  )

load_airport_table = CopyTableOperator(
  task_id = 'Load_airport_table',
  dag=dag,
  table = 'airport',
  schema ='public',
  s3_bucket = 'uda-capstone-data',
  s3_load_prefix = 'csv_data',
  csv_file_name = 'airport-codes_csv.csv'
  )
clean_airport_table = PostgresOperator(
  task_id = 'Clean_airport_table',
  dag=dag,
  postgres_conn_id='redshift',
  sql=SqlQueries.clean_airport_table
)

data_quality_check_on_airport= CheckQualityOperator(
        task_id="Check_data_quality_on_airport",
        dag=dag,
        table='airport'
    )


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


start_operator >> extract_sas_data_operator


extract_sas_data_operator >> create_immigration_table


extract_sas_data_operator >> create_i94cit_i94res_table
extract_sas_data_operator >> create_i94mode_table
extract_sas_data_operator >> create_i94addr_table
extract_sas_data_operator >> create_i94visa_table
extract_sas_data_operator >> create_i94port_table
extract_sas_data_operator >> create_us_cities_demographics_table
extract_sas_data_operator >> create_airport_table
# extract_sas_data_operator >> create_us_state_race_table
# extract_sas_data_operator >> create_us_cities_table

create_immigration_table >> load_immigration_table
create_i94cit_i94res_table >> load_i94cit_i94res_table
create_i94mode_table  >> load_i94mode_table
create_i94addr_table >> load_i94addr_table
create_i94visa_table >> load_i94visa_table
create_i94port_table >> load_i94port_table
create_us_cities_demographics_table >> load_us_cities_demographics_table
create_airport_table >> load_airport_table

load_us_cities_demographics_table >> create_us_state_race_table
load_us_cities_demographics_table >> create_us_cities_table

create_us_state_race_table >> load_us_state_race_table
create_us_cities_table >> load_us_cities_table

load_immigration_table >> data_quality_check_on_immigration
load_i94cit_i94res_table >> data_quality_check_on_i94cit_i94res
load_i94mode_table >> data_quality_check_on_i94mode
load_i94addr_table >> data_quality_check_on_i94addr
load_i94visa_table >> data_quality_check_on_i94visa

load_i94port_table >>  clean_airport_table   
clean_airport_table >> data_quality_check_on_i94port

load_us_cities_demographics_table >> data_quality_check_on_us_cities_demographics
load_airport_table >> data_quality_check_on_airport
load_us_state_race_table >> data_quality_check_on_us_state_race
load_us_cities_table >> data_quality_check_on_us_cities


data_quality_check_on_immigration >> end_operator
data_quality_check_on_immigration >> end_operator
data_quality_check_on_i94mode >> end_operator
data_quality_check_on_i94addr >> end_operator
data_quality_check_on_i94visa >> end_operator
data_quality_check_on_i94port >> end_operator
data_quality_check_on_us_cities_demographics >> end_operator
data_quality_check_on_airport  >> end_operator
data_quality_check_on_i94cit_i94res >> end_operator

data_quality_check_on_us_state_race >> end_operator
data_quality_check_on_us_cities >> end_operator