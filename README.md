# Udacity-DataEngineering-NanoDegree

Projects and resources developed in the [DEND Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027) from Udacity.

## Project 1: [Relational Databases - Data Modeling with PostgreSQL](https://github.com/harshkavdikar1/Udacity-DataEngineering-NanoDegree/tree/master/Data-Modeling-With-Postgres)

<p align="center"><img src="https://github.com/harshkavdikar1/Udacity-DataEngineering-NanoDegree/blob/master/Images/postgres_logo.png" style="height: 100%; width: 100%; max-width: 200px" /></p>

Developed a relational database using PostgreSQL to model user activity data for a music streaming app. Skills include:
* Created a relational database using PostgreSQL
* Developed a Star Schema database using optimized definitions of Fact and Dimension tables. Normalization of tables.
* Built out an ETL pipeline to optimize queries in order to understand what songs users listen to.

Proficiencies include: Python, PostgreSql, Star-Schema, ETL pipelines, Normalization, Denormalization


## Project 2: [NoSQL Databases - Data Modeling with Apache Cassandra](https://github.com/harshkavdikar1/Udacity-DataEngineering-NanoDegree/tree/master/Data-Modeling-With-Cassandra)

<p align="center"><img src="https://github.com/harshkavdikar1/Udacity-DataEngineering-NanoDegree/blob/master/Images/cassandra_logo.png" style="height: 100%; width: 100%; max-width: 200px" /></p>

Designed a NoSQL database using Apache Cassandra based on the original schema outlined in project one. Skills include:
* Created a nosql database using Apache Cassandra (both locally and with docker containers)
* Developed denormalized tables optimized for a specific set queries and business needs

Proficiencies used: Python, Apache Cassandra, Denormalization


## Project 3: [Data Warehouse - Amazon Redshift](https://github.com/harshkavdikar1/Udacity-DataEngineering-NanoDegree/tree/master/Data-Warehouse-ON-AWS)

<p align="center"><img src="https://github.com/harshkavdikar1/Udacity-DataEngineering-NanoDegree/blob/master/Images/redshift_logo.png" style="height: 100%; width: 100%; max-width: 200px" /></p>

Created a database warehouse utilizing Amazon Redshift. Skills include:
* Creating a Redshift Cluster, IAM Roles, Security groups.
* Develop an ETL Pipeline that copies data from S3 buckets into staging tables to be processed into a star schema
* Developed a star schema with optimization to specific queries required by the data analytics team.

Proficiencies used: Python, Amazon Redshift, AWS S3, Amazon SDK (boto3), PostgreSQL, AWS IAM

## Project 4: [Data Lake - Spark](https://github.com/harshkavdikar1/Udacity-DataEngineering-NanoDegree/tree/master/Data-Lake-With-Spark)

<p align="center"><img src="https://github.com/harshkavdikar1/Udacity-DataEngineering-NanoDegree/blob/master/Images/spark_logo.png" style="height: 100%; width: 100%; max-width: 200px" /></p>

Scaled up the current ETL pipeline by moving the data warehouse to a data lake. Skills include:
* Create an EMR Hadoop Cluster
* Further develop the ETL Pipeline copying datasets from S3 buckets, data processing using Spark and writing to S3 buckets using efficient partitioning and parquet formatting.
* Fast-tracking the data lake buildout using (serverless) AWS Lambda and cataloging tables with AWS Glue Crawler.

Technologies used: Spark(pyspark), S3, AWS EMR, Python, Data Lakes.

## Project 5: [Data Pipelines - Airflow](https://github.com/harshkavdikar1/Udacity-DataEngineering-NanoDegree/tree/master/Data-Pipelines-with-Apache-Airflow)

<p align="center"><img src="https://github.com/harshkavdikar1/Udacity-DataEngineering-NanoDegree/blob/master/Images/airflow_logo.png" style="height: 100%; width: 100%; max-width: 200px" /></p>

Automate the ETL pipeline and creation of data warehouse using Apache Airflow. Skills include:
* Using Airflow to automate ETL pipelines using Airflow, Python, Amazon Redshift.
* Writing custom operators to perform tasks such as staging data, filling the data warehouse, and validation through data quality checks.
* Transforming data from various sources into a star schema optimized for the analytics team's use cases.

Technologies used: Apache Airflow, S3, Amazon Redshift, Python.