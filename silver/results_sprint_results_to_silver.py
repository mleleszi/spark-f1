# Databricks notebook source
# MAGIC %md
# MAGIC # Create silver tables for results and sprint_results
# MAGIC 
# MAGIC TODO
# MAGIC convert lap times and intervals

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

results_df = spark.table('marci_dev.bronze.results')
sprint_results_df = spark.table('marci_dev.bronze.sprint_results')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest into results silver table
# MAGIC - Trim string columns.
# MAGIC - Convert column names to snake case.
# MAGIC - Dedupe based on race_id and driver_id.
# MAGIC - Replace '\N' values with null.
# MAGIC - Create table if not exists, merge otherwise.

# COMMAND ----------

results_trimmed_df = trim_cols(results_df, ['positionText'])
results_renamed_df = col_names_to_snake(results_trimmed_df)
results_silver_df = (results_renamed_df
                               .dropDuplicates(['race_id', 'driver_id'])
                               .na.replace('\\N', None))

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
