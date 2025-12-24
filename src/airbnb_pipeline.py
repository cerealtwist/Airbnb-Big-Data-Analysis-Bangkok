from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    avg,
    count,
    sum,
    when,
    regexp_replace
)

# 1. Start Session Spark
def main():
    spark = (
        SparkSession.builder
        .appName("Airbnb Bangkok Big Data Analysis")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
	.config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    # =====================
    # DATA INGESTION
    # =====================

    listings = spark.read.csv(
        "data/listings.csv.gz",
        header=True,
        inferSchema=True,
        multiLine=True,
        quote='"',
        escape='"'
    )

    calendar = spark.read.csv(
        "data/calendar.csv.gz",
        header=True,
        inferSchema=True
    )

    # =====================
    # PREPROCESSING
    # =====================

    calendar_clean = (
        calendar
        .withColumn(
            "price_clean",
            regexp_replace(col("price"), "[\\$,]", "").cast("double")
        )
        .withColumn("date_parsed", to_date(col("date")))
        .withColumn("year", year(col("date_parsed")))
        .withColumn("month", month(col("date_parsed")))
        .select(
            "listing_id",
            "year",
            "month",
            "price_clean",
            "available"
        )
    )

    listings_clean = listings.select(
        col("id").alias("listing_id"),
        col("neighbourhood_cleansed").alias("neighbourhood"),
        col("room_type")
    )

    # =====================
    # JOIN & AGGREGATION
    # =====================

    joined_df = calendar_clean.join(
        listings_clean,
        on="listing_id",
        how="inner"
    )

    result_df = (
        joined_df
        .groupBy("neighbourhood", "year", "month")
        .agg(
            avg("price_clean").alias("avg_price"),
            count("*").alias("total_days"),
            sum(
                when(col("available") == "f", 1).otherwise(0)
            ).alias("booked_days")
        )
        .withColumn(
            "occupancy_rate",
            col("booked_days") / col("total_days")
        )
        .orderBy("neighbourhood", "year", "month")
    )

    # =====================
    # OUTPUT
    # =====================

    result_df.show(10, truncate=False)

    result_df.write \
        .mode("overwrite") \
        .parquet("output/airbnb_bangkok_monthly")

    spark.stop()


if __name__ == "__main__":
    main()

