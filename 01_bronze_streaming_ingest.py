# Databricks notebook source
# MAGIC %md
# MAGIC ### Load To Bronze

# COMMAND ----------

df = spark.read.format("parquet").option('inferSchema','true').load(
    "abfss://rawnyctaxy@azdatalakesynapsean.dfs.core.windows.net/trip-data/*.parquet"
)
# df.write.format("delta").mode("overwrite").save("/mnt/workshop/processed/drivers)

# COMMAND ----------

display(df)

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://rawnyctaxy@azdatalakesynapsean.dfs.core.windows.net/trip-data/"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####load to bronze using _autoloader_

# COMMAND ----------

dbutils.fs.rm(
    "abfss://rawnyctaxy@azdatalakesynapsean.dfs.core.windows.net/trip-data/checkpoint",
    True
)

# COMMAND ----------

df = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet').option('inferSchema','true')\
    .option('cloudFiles.schemaLocation','abfss://rawnyctaxy@azdatalakesynapsean.dfs.core.windows.net/trip-data/schema')\
        .option('cloudFiles.maxFilesPerTrigger','1') \
        .load('abfss://rawnyctaxy@azdatalakesynapsean.dfs.core.windows.net/trip-data/*.parquet')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df.withColumn('updated_date', current_timestamp()).writeStream.format('delta').option('checkpointLocation','abfss://rawnyctaxy@azdatalakesynapsean.dfs.core.windows.net/trip-data/checkpoint_v2').option("mergeSchema", "true").outputMode('append').trigger(availableNow=True).toTable('dev_etl.bronze.nyctaxi')

# COMMAND ----------

df = spark.read.table('dev_etl.bronze.nyctaxi')
# display(df.count())


# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

df.select('VendorID').distinct().count()

# COMMAND ----------

dedup = df.dropDuplicates()
display(dedup.count())

# COMMAND ----------

df.printSchema

# COMMAND ----------

from pyspark.sql.functions import avg, col, desc, row_number,dense_rank, sum
from pyspark.sql.window import Window
dfgrp = df.groupby('VendorID').agg(sum('total_amount').alias('sum_total_amount'))
window_spec = Window.orderBy(col('sum_total_amount').desc())
dfpart = dfgrp.withColumn('rnk', dense_rank().over(window_spec)).filter(col('rnk') <= 3)
display(dfpart)