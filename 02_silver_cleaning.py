# Databricks notebook source
# MAGIC %md
# MAGIC ### Dedup, cleanse, Refine the upstream data from bronze layer

# COMMAND ----------

df_taxy = spark.read.table('dev_etl.bronze.nyctaxi')
dedup_df = df_taxy.dropDuplicates()
dedup_df.count()

# COMMAND ----------

dedup_df.printSchema()

# COMMAND ----------

imputed_df = dedup_df.fillna({"passenger_count": 1,
    "RatecodeID": 1})

# COMMAND ----------

from pyspark.sql.functions import col

silver_df = (
    imputed_df
    .withColumn("passenger_count", col("passenger_count").cast("int"))
    .withColumn("trip_distance", col("trip_distance").cast("double"))
)

# COMMAND ----------

silver_df = silver_df.filter(
    (col("fare_amount") >= 0) &
    (col("trip_distance") > 0) &
    (col("total_amount") >= 0)
)

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp

silver_df = silver_df.withColumn(
    "trip_duration_minutes",
    (unix_timestamp("tpep_dropoff_datetime") -
     unix_timestamp("tpep_pickup_datetime")) / 60
)

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, hour,when

silver_df = (
    silver_df
    .withColumn("pickup_year", year("tpep_pickup_datetime"))
    .withColumn("pickup_month", month("tpep_pickup_datetime"))
    .withColumn("pickup_day", dayofmonth("tpep_pickup_datetime"))
    .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
)

# COMMAND ----------

silver_df.select('payment_type').distinct().show()

# COMMAND ----------

from pyspark.sql.functions import when
silver_df = (
    silver_df.withColumn('payment_type_desc', when(col('payment_type') == 1, 'Credit Card')
    .when(col('payment_type') == 2, 'Cash')
    .otherwise('Other')
    )
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

silver_df = (
    silver_df
    .withColumn("created_date", current_timestamp())
    .withColumn("updated_date", current_timestamp())
    
)

# COMMAND ----------

silver_df = (
    silver_df
    .withColumn("year", year("tpep_pickup_datetime"))
    .withColumn("month", month("tpep_pickup_datetime"))
)

# COMMAND ----------



# COMMAND ----------

# Write Silver DataFrame as an External Delta Table in Unity Catalog

(
    silver_df.write
             .format("delta")
             .mode("overwrite")
             .option("path", "abfss://silver@azdatalakesynapsean.dfs.core.windows.net/silver_nyctaxi/")
             .saveAsTable("dev_etl.silver.nyctaxi")
)