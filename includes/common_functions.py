# Databricks notebook source
# MAGIC %md
# MAGIC # Common Functions

# COMMAND ----------

import re
from pyspark.sql.functions import regexp_replace, col

def camel_to_snake(str):
  """Converts a string to snake case."""
  
  str = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', str)
  return re.sub('([a-z0-9])([A-Z])', r'\1_\2', str).lower()

def col_names_to_snake(df):
  """Converts all column names of a DataFrame to snake case."""
  
  return df.select([col(c).alias(camel_to_snake(c)) for c in df.columns])

# COMMAND ----------

from pyspark.sql.functions import trim

def trim_cols(df, col_names):
  """Trims the specified columns."""
  
  for col_name in col_names:
    df = df.withColumn(col_name, trim(col_name))
    
  return df

# COMMAND ----------

from pyspark.sql.functions import expr

def add_string_time_to_date(df, date_col, time_col, new_col_name):
  """Adds a string time in the format of 'hh:mm:ss' to a date column, returning a new column name new_col_name."""
  
  return df.withColumn(new_col_name, col(date_col).cast('timestamp') + expr(f'make_dt_interval(0, substring({time_col}, 1, 2), substring({time_col}, 4, 2))'))

# COMMAND ----------


