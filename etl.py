import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
# os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell"


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
    song_data = input_data + "song-data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    file_name = output_data + "songs.parquet"
    songs_table.write.partitionBy(["year", "artist_id"]).parquet(file_name)

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_longitude", "artist_latitude", "artist_location")
    
    # write artists table to parquet files
    file_name = output_data + "artists.parquet"
    artists_table.write.parquet(file_name)


def process_log_data(spark, input_data, output_data):
    """
    extract songs data from input json files, transform it and load it to s3
    Args:
        spark : object of spark context
        input_data : path of input_data stored in s3
        output_data : path where output files need to be stored
    """
    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    # rename the columns as per requirements
    df = df.filter("page='NextSong'")\
        .withColumnRenamed("userId", "user_id")\
        .withColumnRenamed("firstName", "first_name")\
        .withColumnRenamed("lastName", "last_name")\
        .withColumnRenamed("sessionId", "session_id")\
        .withColumnRenamed("userAgent", "user_agent")

    # extract columns for users table    
    users_table = df.select("user_id", "first_name", "last_name", "gender", "level")
    
    # write users table to parquet files
    file_name = output_data + "users.parquet"
    users_table.write.parquet(file_name)

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: int(int(x)/1000))
    df = df.withColumn("timestamp", get_timestamp(df.ts))\
        .withColumn("datetime", F.from_unixtime("timestamp", "MM-dd-yyyy HH:mm:ss"))\
        .withColumn("start_time", F.to_timestamp("datetime", "MM-dd-yyyy HH:mm:ss"))\
        .withColumn("month", F.month("start_time"))\
        .withColumn("year", F.year("start_time"))\
        .withColumn("week", F.weekofyear("start_time"))\
        .withColumn("day", F.dayofmonth("start_time"))\
        .withColumn("weekday", F.dayofweek("start_time"))\
        .withColumn("hour", F.hour("start_time"))
    
    # extract columns to create time table
    time_table = df.select("start_time", "month", "year", "week", "day", "weekday", "hour")
    
    # write time table to parquet files partitioned by year and month
    file_name = output_data + "time.parquet"
    time_table.write.partitionBy(["year", "month"]).parquet(file_name)

    # read in song data to use for songplays table
    file_name = output_data + "songs.parquet"
    songs_df = spark.read.parquet(file_name)

    # Create views to perform sql query
    songs_df.createOrReplaceTempView("songs_data")
    df.createOrReplaceTempView("logs_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                    SELECT DISTINCT start_time, user_id, level, song_id, artist_id,
                                    session_id, location, user_agent, logs.year, month
                    FROM logs_data as logs
                    LEFT OUTER JOIN songs_data as songs
                    ON logs.song = songs.title
                    AND logs.length = songs.duration
                    """)

    # Create a column songplays_id and assign it values using monotonically_increasing_id method
    songplays_table = songplays_table.withColumn("songplays_id", F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    file_name = output_data + "songplays.parquet"
    songplays_table.write.partitionBy(["year", "month"]).parquet(file_name)


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

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
