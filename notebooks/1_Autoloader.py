# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Data Loading using AutoLoader

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS netflix_catalog.net_schema;

# COMMAND ----------

# DBTITLE 1,Cell 3
schema_location = "abfss://silver@netflixprojectdlsasi.dfs.core.windows.net/autoloader/schema/netflix_titles"
checkpoint_location = "abfss://silver@netflixprojectdlsasi.dfs.core.windows.net/autoloader/checkpoint/netflix_titles"
source_path = "abfss://raw@netflixprojectdlsasi.dfs.core.windows.net"
sink_path = "abfss://bronze@netflixprojectdlsasi.dfs.core.windows.net/netflix_titles"

# COMMAND ----------

# DBTITLE 1,Cell 4
df = spark.readStream\
  .format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.includeExistingFiles", "true")\
  .option("header", "true")\
  .option("pathGlobFilter", "*.csv")\
  .option("cloudFiles.schemaLocation", schema_location)\
  .load(source_path)

# COMMAND ----------

# DBTITLE 1,Cell 5
display(df)

# COMMAND ----------

# DBTITLE 1,Cell 6
stream_query = df.writeStream\
  .format("delta")\
  .option("checkpointLocation", checkpoint_location)\
  .trigger(availableNow=True)\
  .start(sink_path)

stream_query.awaitTermination()