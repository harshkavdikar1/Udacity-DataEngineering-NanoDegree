# Data Warehouse ON AWS
```
In this project we are creating an ETL pipeline to create and popuate the star schema with data
in Postgres to optimize the data analytics for Sparkify. The data is present in S3 in series of
json files. Song data has prefix of song-data and log-data has prefix of log data. Our application
will extract data from S3 and load it to the staging tables in redshift. From staging tables in
redshift we will perform transformations and load it to the target tables.

We have the following star schema for our tables.

Fact Table:
    Fact table stores the quantifiable metrics which can be aggreagted to get useful results.
    We have following fact table.
    1. songplays - records in log data associated with song plays i.e. records with page = NextSong

Dimension Tables:
    Dimension Tables stores the business events related to the facts. We have following dimension tables.
    1. users - users in the app
    
    2. songs - songs in music database

    3. artists - artists in music database

    4. time - timestamps of records in songplays broken down into specific units

We have the following files in the etl process:
1. create_tables.py: Used to execute the sql queries to create the tables in database
2. sql_queries.py: Contains all the sql queries used to create tables and insert data to it.
3. etl.py: ETL pipeline to execute the commands to redshift to extract data from S3, 
   transform it and load it into target tables.
4. download_resources.py: To download the input files from internet.
5. upload_resources.py: To upload the input files to S3 bucket.
```

## ⚙  How to run your program
* Install the dependencies. 
```
pip install -r requirements.txt
```
* Create an IAM role for redshift with read only access to S3 then create a redshift cluster and update the conf 
  file with parameters, then execute follwing python script to create tables in redshift.
```
python create_tables.py
```
* Download resoucres for the project.
```
python download_resources.py
```
* Create a S3 bucket and IAM role with full access to S3, modify the below file with aws_acces_id and aws_acces_key
  and run below python script to upload the resources to the S3 bucket.
```
python upload_resources.py
```
* Run the ETL pipeline.
```
python etl.py
```
