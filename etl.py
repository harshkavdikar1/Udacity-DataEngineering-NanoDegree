import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell"


def create_spark_session():
    """
    Create the spark session and returns its object
    Return Type:
        SparkSession: object of spark sesion
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    extract songs data from input json files, transform it and load it to s3
    Args:
        spark : object of spark context
        input_data : path of input_data stored in s3
        output_data : path where output files need to be stored
    """

    # get filepath to song data file
    song_data = input_data + "/song-data/*/*/*/*.json"

    # read song data file
    df = 

    # extract columns to create songs table
    songs_table = 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table

    # extract columns to create artists table
    artists_table = 
    
    # write artists table to parquet files
    artists_table


def process_log_data(spark, input_data, output_data):
    """
    extract songs data from input json files, transform it and load it to s3
    Args:
        spark : object of spark context
        input_data : path of input_data stored in s3
        output_data : path where output files need to be stored
    """
    # get filepath to log data file
    log_data = 

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    """
    Extract the source data from s3

    Transform the data using spark to create star schema

    Create fact table - songplays

    Create dimention tables - songs, time, artists, users

    Load the data back to s3 in parquet format
    """
    spark = create_spark_session()

    input_data = "s3a://udacitydenanodegree2020/"
    output_data = "s3a://udacitydenanodegree2020/output/"
    input_data = "data"
    output_data = "data"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
