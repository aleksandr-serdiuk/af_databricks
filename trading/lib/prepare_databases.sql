-- Databricks notebook source
DROP DATABASE IF EXISTS silver CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS silver
LOCATION "/mnt/serdiukstorage/silver";

-- COMMAND ----------

DROP DATABASE IF EXISTS gold CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS gold
LOCATION "/mnt/serdiukstorage/gold";
