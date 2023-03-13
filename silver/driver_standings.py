# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Create driver_standings silver table

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

results_df = spark.table('marci_dev.silver.results')
sprint_results_df = spark.table('marci_dev.silver.sprint_results')

# COMMAND ----------

from pyspark.sql.functions import concat, lit, expr, col, coalesce

driver_standings_df = (results_df
                          .withColumnRenamed('points', 'race_points')
                          .withColumnRenamed('position', 'race_position')
                          .join(sprint_results_df.withColumnRenamed('points', 'sprint_points'), ['driver_id', 'race_id'], 'outer')
                          .withColumn('points', col('race_points') + coalesce(col('sprint_points'), lit(0)))
                          .select('race_id', 'driver_id', 'points', 'race_position'))

merge_condition = 'target.race_id = source.race_id AND target.driver_id = source.driver_id'
merge_delta(driver_standings_df, 'marci_dev.silver.driver_standings', merge_condition)
