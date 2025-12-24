import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, min, max, count
)

def main():
    spark = SparkSession.builder \
        .appName("AirbnbBangkok_Q4_Seasonality") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # ------------------------------------------------------------------
    # 1. LOAD CLEAN PARQUET OUTPUT
    # ------------------------------------------------------------------
    df = spark.read.parquet(
        "file:///home/harry/Airbnb-Big-Data-Analysis-Bangkok/output/airbnb_bangkok_monthly"
    )

    # ------------------------------------------------------------------
    # 2. CITY-WIDE SEASONALITY (MONTHLY PATTERN)
    # ------------------------------------------------------------------
    print("\nCity-wide Monthly Seasonality:")
    monthly_seasonality = df.groupBy("month") \
        .agg(
            avg("occupancy_rate").alias("avg_city_occupancy"),
            stddev("occupancy_rate").alias("stddev_city_occupancy"),
            count("*").alias("observations")
        ) \
        .orderBy("month")

    monthly_seasonality.show(12, truncate=False)

    # ------------------------------------------------------------------
    # 3. NEIGHBOURHOOD SEASONAL SENSITIVITY
    # ------------------------------------------------------------------
    print("\nNeighbourhood Seasonal Sensitivity:")
    neighbourhood_seasonality = df.groupBy("neighbourhood") \
        .agg(
            avg("occupancy_rate").alias("avg_occupancy"),
            (max("occupancy_rate") - min("occupancy_rate")).alias("seasonal_range"),
            stddev("occupancy_rate").alias("stddev_occupancy"),
            count("*").alias("observations")
        ) \
        .orderBy(col("seasonal_range").desc())

    neighbourhood_seasonality.show(20, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()

