# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest all files to bronze tables

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def autoload_to_table(file_name, data_source, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", 'csv')
                  .option("cloudFiles.schemaLocation", f'{checkpoint_directory}/bronze/{file_name}')
                  .option('pathGlobfilter', f'{file_name}*')
                  .load(data_source)
                  .withColumn('ingestion_date', current_timestamp())
                  .writeStream
                  .option("checkpointLocation", f'{checkpoint_directory}/bronze/{file_name}')
                  .option("mergeSchema", "true")
                  .table(f'marci_dev.bronze.{file_name}'))
    return query

# COMMAND ----------

source_directory = '/mnt/marcidev/raw'
checkpoint_directory = '/user/marcell.leleszi@datapao.com/f1/_checkpoint'

# COMMAND ----------

autoload_to_table('drivers', source_directory, checkpoint_directory)

# COMMAND ----------

autoload_to_table('constructors', source_directory, checkpoint_directory)

# COMMAND ----------

autoload_to_table('circuits', source_directory, checkpoint_directory)

# COMMAND ----------

autoload_to_table('races', source_directory, checkpoint_directory)

# COMMAND ----------

autoload_to_table('qualifying', source_directory, checkpoint_directory)

# COMMAND ----------

autoload_to_table('sprint_results', source_directory, checkpoint_directory)

# COMMAND ----------

autoload_to_table('status', source_directory, checkpoint_directory)

# COMMAND ----------

autoload_to_table('results', source_directory, checkpoint_directory)

# COMMAND ----------

autoload_to_table('pit_stops', source_directory, checkpoint_directory)

# COMMAND ----------

autoload_to_table('lap_times', source_directory, checkpoint_directory)
