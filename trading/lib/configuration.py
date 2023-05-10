# Databricks notebook source
bronze_folder_path = "/mnt/serdiukstorage/bronze"
silver_folder_path = "/mnt/serdiukstorage/silver"
gold_folder_path = "/mnt/serdiukstorage/gold"

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "EET")
