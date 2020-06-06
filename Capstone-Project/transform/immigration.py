from pyspark.sql import SparkSession

spark = SparkSession.builder.\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()

# df_spark = spark.read.format("com.github.saurfang.sas.spark")\
#     .load("../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat")

df_spark = spark.read.parquet("input data/*.parquet")


df_immigration = df_spark.selectExpr("cast(cicid as int) cicid",
                                     "cast(i94res as int) country_code",
                                     "i94port as iata_code",
                                     "i94addr as state_code",
                                     "cast(i94bir as int) age",
                                     "visapost as visa_issue_port",
                                     "occup as occupation",
                                     "cast(biryear as int) birth_year",
                                     "gender",
                                     "visatype as visa_type")

df_immigration.write.parquet(
    "Capstone-Project/staging_data/immigration.parquet")
