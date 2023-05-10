# Databricks notebook source
# MAGIC %run "../../lib/configuration"

# COMMAND ----------

# MAGIC %run "../../lib/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

country_schema = StructType(fields = [
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("code", StringType(), True),
    StructField("code_2l", StringType(), True),
    StructField("code_alpha2", StringType(), True),
    StructField("phone_code", IntegerType(), True),
    StructField("eu", IntegerType(), True),
    StructField("ft", IntegerType(), True),
    StructField("uk", IntegerType(), True),
    StructField("formatted_name", StringType(), True),
    StructField("iso_code", IntegerType(), True)
])

# COMMAND ----------

country_df = spark.read \
    .schema(country_schema) \
    .parquet(f"{bronze_folder_path}/crm/country.parquet") \
    .dropDuplicates(['id']) \
    .withColumnRenamed("id", "country_id") \
    .withColumnRenamed("name", "country_name") \
    .withColumnRenamed("code", "country_code") \
    .withColumnRenamed("formatted_name", "country_formatted_name") \
    .select(['country_id', 'country_name', 'country_code', 'country_formatted_name']) \
    .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

#display(country_df)

# COMMAND ----------

country_df.write.mode("overwrite").format("delta").saveAsTable("silver.country")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc EXTENDED silver.country
