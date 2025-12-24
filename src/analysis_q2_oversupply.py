from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

spark = SparkSession.builder \
    .appName("AirbnbBangkok_Q2_Oversupply") \
    .getOrCreate()

df = spark.read.parquet("file:///home/harry/Airbnb-Big-Data-Analysis-Bangkok/output/airbnb_bangkok_monthly")

oversupply = df.groupBy("neighbourhood") \
    .agg(
        avg("occupancy_rate").alias("avg_occupancy"),
        count("*").alias("observations")
    ) \
    .orderBy("avg_occupancy")

oversupply.show(15, truncate=False)

