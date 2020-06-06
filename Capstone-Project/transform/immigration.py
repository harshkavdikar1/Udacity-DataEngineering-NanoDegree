from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType
from datetime import datetime, timedelta

spark = SparkSession.builder.\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()

# df_spark = spark.read.format("com.github.saurfang.sas.spark")\
#     .load("../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat")

df_spark = spark.read.parquet("input data/*.parquet")

mapping_visa_mode = {1: "Business", 2: "Pleasure", 3: "Student"}
mapping_arrival_mode = {1: 'Air', 2: 'Sea', 3: 'Land', 9: 'Not reported'}

get_visa_mode = udf(lambda x: mapping_visa_mode.get(x))
get_arrival_mode = udf(lambda x: mapping_arrival_mode.get(x))


def to_datetime(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None


convert_sas_to_datetime = udf(lambda x: to_datetime(x), DateType())


def to_datetimefrstr(x):
    try:
        return datetime.strptime(x, '%m%d%Y')
    except:
        return None


convert_str_to_datetime = udf(lambda x: to_datetimefrstr(x), DateType())

df_immigration = df_spark.withColumn("visa_mode", get_visa_mode("i94visa"))\
    .withColumn("arrival_mode", get_arrival_mode("i94mode"))\
    .withColumn("departure_date", convert_sas_to_datetime("depdate"))\
    .withColumn("arrival_date", convert_sas_to_datetime("arrdate"))\
    .withColumn("visa_expiration_date", convert_str_to_datetime("dtaddto"))\
    .selectExpr("cast(cicid as int) cicid",
                "cast(i94res as int) country_code",
                "i94port as iata_code",
                "i94addr as state_code",
                "cast(i94bir as int) age",
                "visapost as visa_issue_port",
                "arrival_date",
                "departure_date",
                "visa_expiration_date",
                "arrival_mode",
                "visa_mode",
                "occup as occupation",
                "cast(biryear as int) birth_year",
                "gender",
                "visatype as visa_type")

df_immigration.write.parquet(
    "Capstone-Project/staging_data/immigration.parquet")
