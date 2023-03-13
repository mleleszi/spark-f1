# Databricks notebook source
# MAGIC %md
# MAGIC # Create silver tables for races and drivers
# MAGIC 
# MAGIC TODO
# MAGIC convert lap times and intervals

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

drivers_df = spark.table('marci_dev.bronze.drivers')
races_df = spark.table('marci_dev.bronze.races')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert into drivers silver table
# MAGIC - Trim string columns.
# MAGIC - Convert column names to snake case.
# MAGIC - Concat forename and surname columns.
# MAGIC - Drop url column.
# MAGIC - Replace '\N' values with null.
# MAGIC - Create table if not exists, merge if exists.

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit

drivers_trimmed_df = trim_cols(drivers_df, ['driverRef', 'code', 'forename', 'surname', 'nationality'])
drivers_renamed_df = col_names_to_snake(drivers_trimmed_df)
drivers_silver_df = (drivers_renamed_df
                                    .withColumn('driver_name', concat(col('forename'), lit(' '), col('surname')))
                                    .drop('url', 'forename', 'surname')
                                    .na.replace('\\N', None))

merge_condition = 'target.driver_id = source.driver_id'
merge_delta(drivers_silver_df, 'marci_dev.silver.drivers', merge_condition)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest into races silver table
# MAGIC - Trim string columns.
# MAGIC - Convert column names to snake case.
# MAGIC - Drop url column.
# MAGIC - Replace '\N' values with null.
# MAGIC - Create timestamp, fp1_timestamp, fp2_timestamp, fp3_timestamp and sprint_timestamp columns.
# MAGIC   - By adding string time column to date column.
# MAGIC - Create table if not exists, merge otherwise.

# COMMAND ----------

races_trimmed_df = trim_cols(races_df, ['name'])
races_renamed_df = col_names_to_snake(races_trimmed_df)

races_timestamp_converted_df = add_string_time_to_date(races_renamed_df, 'date', 'time', 'timestamp')
races_timestamp_converted_df = add_string_time_to_date(races_timestamp_converted_df, 'fp1_date', 'fp1_time', 'fp1_timestamp')
races_timestamp_converted_df = add_string_time_to_date(races_timestamp_converted_df, 'fp2_date', 'fp2_time', 'fp2_timestamp')
races_timestamp_converted_df = add_string_time_to_date(races_timestamp_converted_df, 'fp3_date', 'fp3_time', 'fp3_timestamp')
races_timestamp_converted_df = add_string_time_to_date(races_timestamp_converted_df, 'sprint_date', 'sprint_time', 'sprint_timestamp')

races_silver_df = (races_timestamp_converted_df
                                        .drop('url')
                                        .na.replace('\\N', None))

merge_condition = 'target.race_id = source.race_id'
merge_delta(races_silver_df, 'marci_dev.silver.races', merge_condition)
