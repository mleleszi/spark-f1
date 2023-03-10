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
