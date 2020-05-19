# Data-Warehouse-on-AWS

# DataWarehouse ON AWS
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
    1. songplays - records in log data associated with song plays i.e. records with page NextSong

Dimension Tables:
    Dimension Tables stores the business events related to the facts. We have following dimension tables.
    1. users - users in the app
    
    2. songs - songs in music database

    3. artists - artists in music database

    4. time - timestamps of records in songplays broken down into specific units

We have the following files in the etl process:
1. create_tables.py: Used to execute the sql queries to create the tables in database
2. sql_queries.py: Contains all the sql queries used to create tables and insert data to it.
3. etl.py: ETL pipeline to read the data from csv files and load them to postgres for analysis.
```

## ⚙  How to run your program
* Install the dependencies 
```
pip install -r requirements.txt
```
* Create tables in the database
```
python create_tables.py
```
* Download resoucres for the project
```
python download_resources.py
```
* Upload the resources to the S#
```
python upload_resources.py
```
* Execute the ETL pipeline
```
python etl.py
```
