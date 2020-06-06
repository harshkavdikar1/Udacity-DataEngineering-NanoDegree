from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

spark = SparkSession.builder.\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()

# df_spark = spark.read.format("com.github.saurfang.sas.spark")\
#     .load("../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat")

df_spark = spark.read.parquet("input data/*.parquet")

mapping_visa_mode = {1: "Business", 2: "Pleasure", 3: "Student"}
mapping_arrival_mode = {1: 'Air', 2: 'Sea', 3: 'Land', 9: 'Not reported'}

get_visa_mode = F.udf(lambda x: mapping_visa_mode.get(x))
get_arrival_mode = F.udf(lambda x: mapping_arrival_mode.get(x))

df_immigration = df_spark.withColumn("visa_mode", get_visa_mode("i94visa"))\
    .withColumn("arrival_mode", get_arrival_mode("i94mode"))\
    .selectExpr("cast(cicid as int) cicid",
                "cast(i94res as int) country_code",
                "i94port as iata_code",
                "i94addr as state_code",
                "cast(i94bir as int) age",
                "visapost as visa_issue_port",
                "arrival_mode",
                "visa_mode",
                "occup as occupation",
                "cast(biryear as int) birth_year",
                "gender",
                "visatype as visa_type")

df_immigration.write.parquet(
    "Capstone-Project/staging_data/immigration.parquet")
