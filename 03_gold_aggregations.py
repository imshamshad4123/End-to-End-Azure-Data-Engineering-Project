# Databricks notebook source
# MAGIC %md
# MAGIC ### Business level Aggregates

# COMMAND ----------

from pyspark.sql.functions import (
    sum,
    avg,
    count,
    max,
    round,
    col
)

# Read Silver Table
silver_df = spark.table("dev_etl.silver.nyctaxi")

# Gold Layer Transformation
# Business Aggregation:
# - Total trips
# - Total revenue
# - Average fare
# - Average trip distance
# - Maximum trip amount
# - Total passengers
# Grouped by pickup year, month, and pickup borough

gold_df = (
    silver_df
        .groupBy(
            "pickup_year",
            "pickup_month"
        )
        .agg(
            count("*").alias("total_trips"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(avg("fare_amount"), 2).alias("avg_fare_amount"),
            round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
            max("total_amount").alias("max_trip_amount"),
            sum("passenger_count").alias("total_passengers")
        )
        .orderBy(
            col("pickup_year"),
            col("pickup_month"),
            col("total_revenue").desc()
        )
)

# Write Gold DataFrame as an External Delta Table
(
    gold_df.write
           .format("delta")
           .mode("overwrite")
           .option(
               "path",
               "abfss://gold@azdatalakesynapsean.dfs.core.windows.net/nyctaxi_summary/"
           )
           .saveAsTable("dev_etl.gold.nyctaxi_summary")
)

# Preview
display(gold_df)