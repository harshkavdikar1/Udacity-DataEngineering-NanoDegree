from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

import re
import s3fs
import pandas as pd

class ExtractionFromSASOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_bucket = '',
                 s3_load_prefix = '',
                 s3_save_prefix = '',
                 file_name = '',
                 aws_conn_id = 'aws_credentials',
                 *args, **kwargs):

        super(ExtractionFromSASOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_load_prefix = s3_load_prefix
        self.s3_save_prefix = s3_save_prefix
        self.file_name = file_name
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        self.log.info("Start extracting dimensional table from .sas file...")

        ## AWS setup
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()

        s3 = s3fs.S3FileSystem(anon=False,
                               key=credentials.access_key,
                               secret=credentials.secret_key)

        self.log.info("Test...")
        self.log.info(self.s3_bucket)
        self.log.info(self.s3_load_prefix)
        
        sas_file_path = "s3://{}/{}/{}".format(self.s3_bucket, self.s3_load_prefix, self.file_name)
        csv_save_path ="s3://{}/{}".format(self.s3_bucket, self.s3_save_prefix)

        self.log.info(f'Loading file from ',sas_file_path)

        with s3.open(sas_file_path, "r") as f:
            file = f.read()

        sas_dict ={}
        temp_data = []

        for line in file.split("\n"):
            line = re.sub(r"\s+", " ", line)
        
        # get description
            if "/*" in line and "-" in line:
                key, value = [i.strip(" ") for i in line.split("*")[1].split("-", 1)]
                key = key.replace(' & ', '_').lower()
                sas_dict[key] = {'description': value}

        # get value in every section
            elif '=' in line and ';' not in line:
                temp_data.append([i.strip(' ').strip("'").title() for i in line.split('=')])
        # save data in sas_dict
            elif len(temp_data) > 0: 
                sas_dict[key]['data'] = temp_data
                temp_data = []
        
        # Get i94cit_i94res
        sas_dict['i94cit_i94res']['df'] = pd.DataFrame(
            sas_dict['i94cit_i94res']['data'], 
            columns=['code', 'country'])
        
        # Get i94port
        tempdf = pd.DataFrame(sas_dict['i94port']['data'],columns=['code', 'port_of_entry'])
        tempdf['code'] = tempdf['code'].str.upper()
        tempdf[['city', 'state_or_country']] = tempdf['port_of_entry'].str.rsplit(',', 1, expand=True)
        sas_dict['i94port']['df'] = tempdf
        
        # Get i94mode
        sas_dict['i94mode']['df'] = pd.DataFrame(sas_dict['i94mode']['data'], columns=['code', 'transportation'])
        
        # Get i94addr
        tempdf = pd.DataFrame(sas_dict['i94addr']['data'],columns=['code', 'state'])
        tempdf['code'] = tempdf['code'].str.upper()
        sas_dict['i94addr']['df'] = tempdf
        
        # Get i94visa
        sas_dict['i94visa']['df'] = pd.DataFrame(sas_dict['i94visa']['data'], columns=['code', 'reason_for_travel'])      
        
        # Write every extracted table into s3 bucket
        for table in sas_dict.keys():
            if 'df' in sas_dict[table].keys():
                self.log.info(f"Writing {table} to S3")
                save_file = csv_save_path + f"/{table}.csv"
                self.log.info(save_file)
                with s3.open(save_file, "w") as f:
                    sas_dict[table]['df'].to_csv(f, index=False)
                    self.log.info(f"Writing {table} successfully into S3 bucket!")
