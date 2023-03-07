# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch Ergast Database

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download and unzip csv files

# COMMAND ----------

# MAGIC %sh
# MAGIC curl http://ergast.com/downloads/f1db_csv.zip --output /tmp/f1db_csv.zip
# MAGIC unzip /tmp/f1db_csv.zip -d /tmp/f1db_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Move files to ADLS

# COMMAND ----------

for file in dbutils.fs.ls('file:/tmp/f1db_csv'):
  dbutils.fs.mv(file.path, f'/mnt/marcidev/raw/{file.name[:-4]}/{file.name}')
