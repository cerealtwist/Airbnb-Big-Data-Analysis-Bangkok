from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def main():
    spark = SparkSession.builder \
        .appName("PriceQualityCheck") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.parquet("file:///home/harry/Airbnb-Big-Data-Analysis-Bangkok/output/airbnb_bangkok_monthly")

    df.select(
        count("*").alias("total_rows"),
        count(col("avg_base_price")).alias("non_null_price")
    ).show()

    spark.stop()

if __name__ == "__main__":
    main()

