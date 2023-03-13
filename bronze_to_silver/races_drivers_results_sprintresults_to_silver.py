# Databricks notebook source
# MAGIC %md
# MAGIC # Create silver tables for races, drivers, results and sprint_results
# MAGIC 
# MAGIC TODO
# MAGIC convert lap times and intervals

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

drivers_df = spark.table('marci_dev.bronze.drivers')
races_df = spark.table('marci_dev.bronze.races')
results_df = spark.table('marci_dev.bronze.results')
sprint_results_df = spark.table('marci_dev.bronze.sprint_results')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert into drivers silver table
# MAGIC - Trim string columns.
# MAGIC - Convert column names to snake case.
# MAGIC - Drop url column.
# MAGIC - Replace '\N' values with null.
# MAGIC - Create table if not exists, merge if exists.

# COMMAND ----------

drivers_trimmed_df = trim_cols(drivers_df, ['driverRef', 'code', 'forename', 'surname', 'nationality'])
drivers_renamed_df = col_names_to_snake(drivers_trimmed_df)
drivers_silver_df = (drivers_renamed_df
                                    .drop('url')
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest into results silver table
# MAGIC - Trim string columns.
# MAGIC - Convert column names to snake case.
# MAGIC - Replace '\N' values with null.
# MAGIC - Create table if not exists, merge otherwise.

# COMMAND ----------

results_trimmed_df = trim_cols(results_df, ['positionText'])
results_renamed_df = col_names_to_snake(results_trimmed_df)
results_silver_df = results_renamed_df.na.replace('\\N', None)

merge_condition = 'target.result_id = source.result_id'
merge_delta(results_silver_df, 'marci_dev.silver.results', merge_condition)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest into sprint_results silver table
# MAGIC - Trim string columns.
# MAGIC - Convert column names to snake case.
# MAGIC - Replace '\N' values with null.
# MAGIC - Create table if not exists, merge otherwise.

# COMMAND ----------

sprint_results_trimmed_df = trim_cols(sprint_results_df, ['positionText'])
sprint_results_renamed_df = col_names_to_snake(sprint_results_trimmed_df)
sprint_results_silver_df = sprint_results_renamed_df.na.replace('\\N', None)

merge_condition = 'target.sprint_result_id = source.sprint_result_id'
merge_delta(sprint_results_silver_df, 'marci_dev.silver.sprint_results', merge_condition)
