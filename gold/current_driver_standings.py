# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Create current_driver_standings gold table

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

driver_standings_df = spark.table('marci_dev.silver.driver_standings')
races_df = spark.table('marci_dev.silver.races')
drivers_df = spark.table('marci_dev.silver.drivers')

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, lit, col, current_date, year
from datetime import date


current_year = date.today().year

current_driver_standings_df = (driver_standings_df
          .join(races_df, 'race_id')
          .join(drivers_df, 'driver_id')
          .filter(f'year = {current_year}')
          .groupBy('driver_name').agg(sum('points').alias('points'),
                                   count(when(col('race_position') == 1, True)).alias('wins'),
                                   count(when(col('race_position') == 2, True)).alias('2nd_places'),
                                   count(when(col('race_position') == 3, True)).alias('3rd_places'))
          .orderBy(col('points').desc(),
                   col('wins').desc(),
                   col('2nd_places').desc(),
                   col('3rd_places').desc())
          .select(col('driver_name'),
                  col('points'),
                  col('wins'),
                  (col('wins') + col('2nd_places') + col('3rd_places')).alias('podiums')))

merge_condition = 'target.driver_name = source.driver_name'
merge_delta(current_driver_standings_df, 'marci_dev.gold.current_driver_standings', merge_condition)
