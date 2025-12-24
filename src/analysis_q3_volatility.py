import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    stddev,
    count
)

def main():
    # ------------------------------------------------------------------
    # 1. Spark Session
    # ------------------------------------------------------------------
    spark = SparkSession.builder \
        .appName("AirbnbBangkok_Q3_Volatility") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    # ------------------------------------------------------------------
    # 2. Load Parquet (FINAL ETL OUTPUT)
    # ------------------------------------------------------------------
    df = spark.read.parquet(
        "file:///home/harry/Airbnb-Big-Data-Analysis-Bangkok/output/airbnb_bangkok_monthly"
    )

    # ------------------------------------------------------------------
    # 3. Volatility Analysis
    # ------------------------------------------------------------------
    volatility_df = df.groupBy("neighbourhood") \
        .agg(
            avg("occupancy_rate").alias("avg_occupancy"),
            stddev("occupancy_rate").alias("stddev_occupancy"),
            count("*").alias("observations")
        ) \
        .orderBy("avg_occupancy", "stddev_occupancy")

    # ------------------------------------------------------------------
    # 4. Output
    # ------------------------------------------------------------------
    print("\nVolatility Analysis (Low Avg Occupancy = Potential Oversupply Risk):")
    volatility_df.show(20, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()

