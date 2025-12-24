from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("AirbnbBangkok_SanityCheck") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()

    df = spark.read.parquet("output/airbnb_bangkok_monthly")

    df.printSchema()
    df.show(5, truncate=False)

    df.selectExpr(
        "min(occupancy_rate)",
        "max(occupancy_rate)",
        "avg(occupancy_rate)"
    ).show()

    spark.stop()

if __name__ == "__main__":
    main()

