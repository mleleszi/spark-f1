# Databricks notebook source
# MAGIC %md
# MAGIC # Create silver tables for races, drivers, results and sprint_results

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

drivers_df = spark.table('marci_dev.bronze.drivers')
races_df = spark.table('marci_dev.bronze.races')
results_df = spark.table('marci_dev.bronze.results')
sprint_results_df = spark.table('marci_dev.bronze.sprint_results')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create drivers silver table
# MAGIC - Trim string columns.
# MAGIC - Convert column names to snake case.
# MAGIC - Drop url column.
# MAGIC - Replace '\N' values with null.

# COMMAND ----------

drivers_trimmed_df = trim_cols(drivers_df, ['driverRef', 'code', 'forename', 'surname', 'nationality'])
drivers_renamed_df = col_names_to_snake(drivers_trimmed_df)
drivers_silver_df = (drivers_renamed_df
                                    .drop('url')
                                    .na.replace('\\N', None))

drivers_silver_df.write.saveAsTable('marci_dev.silver.drivers')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create races silver table
# MAGIC - Trim string columns.
# MAGIC - Convert column names to snake case.
# MAGIC - Drop url column.
# MAGIC - Replace '\N' values with null.
# MAGIC - Create timestamp, fp1_timestamp, fp2_timestamp, fp3_timestamp and sprint_timestamp columns.
# MAGIC   - By adding string time column to date column.

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

races_silver_df.write.saveAsTable('marci_dev.silver.races')
