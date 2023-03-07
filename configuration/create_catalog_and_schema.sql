-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS marci_dev

-- COMMAND ----------

USE CATALOG marci_dev;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
