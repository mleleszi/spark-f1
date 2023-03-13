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

# MAGIC %md
# MAGIC ### Create schema from YAML configuration file

# COMMAND ----------

import yaml
with open('./schema.yaml') as file:
  schema_yaml = yaml.safe_load(file)

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, FloatType, StringType, DateType, TimestampType

types = {
  'int': IntegerType,
  'string': StringType,
  'float': FloatType,
  'date': DateType,
  'timestamp': TimestampType
}

schema = StructType()

for k, v in schema_yaml['schema'][table_name].items():
  schema.add(k, types[v['type']](), v['nullable'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in file with AutoLoader and save to bronze table

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def autoload_to_table(source_directory, checkpoint_directory, table_name, schema):
    query = (spark.readStream
                  .format('cloudFiles')
                  .option('cloudFiles.format', 'csv')
                  .option('header', True)
                  .schema(schema)
                  .load(source_directory)
                  .withColumn('ingestion_date', current_timestamp())
                  .writeStream
                  .trigger(availableNow=True)
                  .option('checkpointLocation', f'{checkpoint_directory}/bronze/{table_name}')
                  .table(f'marci_dev.bronze.{table_name}'))
    return query

# COMMAND ----------

autoload_to_table(source_directory, checkpoint_directory, table_name, schema)
