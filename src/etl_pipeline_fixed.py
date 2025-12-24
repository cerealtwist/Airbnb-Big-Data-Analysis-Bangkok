import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month,
    avg, count, sum, when, regexp_replace, lit
)

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    listings_path = f"file://{os.path.join(base_dir, 'data', 'listings.csv.gz')}"
    calendar_path = f"file://{os.path.join(base_dir, 'data', 'calendar.csv.gz')}"
    output_path = f"file://{os.path.join(base_dir, 'output', 'airbnb_bangkok_monthly')}"

    spark = SparkSession.builder \
        .appName("AirbnbBangkok_ETL_Fixed") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()

    # --- READ LISTINGS ---
    listings = spark.read.csv(
        listings_path,
        header=True,
        inferSchema=True,
        multiLine=True,
        quote='"',
        escape='"'
    )

    listings_clean = listings.withColumn(
        "base_price",
        regexp_replace(col("price"), "[\\$,]", "").cast("double")
    ).select(
        col("id").alias("listing_id"),
        col("neighbourhood_cleansed").alias("neighbourhood"),
        "base_price"
    )

    # --- READ CALENDAR ---
    calendar = spark.read.csv(calendar_path, header=True, inferSchema=True)

    calendar_clean = calendar \
        .withColumn("date_parsed", to_date(col("date"))) \
        .withColumn("year", year(col("date_parsed"))) \
        .withColumn("month", month(col("date_parsed")))

    # --- JOIN ---
    joined = calendar_clean.join(
        listings_clean,
        on="listing_id",
        how="inner"
    )

    # --- AGGREGATION ---
    result = joined.groupBy("neighbourhood", "year", "month") \
        .agg(
            avg("base_price").alias("avg_base_price"),
            count("*").alias("total_days"),
            sum(when(col("available") == "f", 1).otherwise(0)).alias("booked_days")
        ) \
        .withColumn("occupancy_rate", col("booked_days") / col("total_days")) \
        .withColumn("price_source", lit("listings_base_price")) \
        .orderBy("neighbourhood", "year", "month")

    result.write.mode("overwrite").parquet(output_path)

    result.show(5, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()

