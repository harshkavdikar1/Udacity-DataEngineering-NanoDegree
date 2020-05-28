# Data-Modeling-With-Postgres
```
In this project we are creating an ETL pipeline to create and popuate the star schema with data
in Postgres to optimize the data analytics for Sparkify. We have the following star schema 

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
* Execute the ETL pipeline
```
python etl.py
```