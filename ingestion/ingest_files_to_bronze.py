# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest files to bronze tables

# COMMAND ----------

dbutils.widgets.text('source_directory', '')
dbutils.widgets.text('checkpoint_directory', '')
dbutils.widgets.text('table_name', '')

# COMMAND ----------

source_directory = dbutils.widgets.get('source_directory')
checkpoint_directory = dbutils.widgets.get('checkpoint_directory')
table_name = dbutils.widgets.get('table_name')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def autoload_to_table(source_directory, checkpoint_directory, table_name):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", 'csv')
                  .option("cloudFiles.schemaLocation", f'{checkpoint_directory}/bronze/{table_name}')
                  .load(source_directory)
                  .withColumn('ingestion_date', current_timestamp())
                  .writeStream
                  .trigger(availableNow=True)
                  .option("checkpointLocation", f'{checkpoint_directory}/bronze/{table_name}')
                  .option("mergeSchema", "true")
                  .table(f'marci_dev.bronze.{table_name}'))
    return query

# COMMAND ----------

autoload_to_table(source_directory, checkpoint_directory, table_name)
