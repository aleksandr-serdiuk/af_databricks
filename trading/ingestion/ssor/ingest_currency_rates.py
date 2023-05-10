# Databricks notebook source
# MAGIC %run "../../lib/configuration"

# COMMAND ----------

# MAGIC %run "../../lib/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat, sha2, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType, DecimalType

# COMMAND ----------

currency_rates_schema = StructType(fields = [
    StructField("currency_date", DateType(), False),
    StructField("src_currency", StringType(), False),
    StructField("dst_currency", StringType(), False),
    StructField("value", DecimalType(30,18), False),
    StructField("source_id", IntegerType(), False),
    StructField("load_ts", TimestampType(), False),
])

# COMMAND ----------

currency_rates_df = spark.read \
    .schema(currency_rates_schema) \
    .parquet(f"{bronze_folder_path}/ssor/currency_rates.parquet") \
    .dropDuplicates(['currency_date', 'src_currency', 'dst_currency']) \
    .select(['currency_date','src_currency','dst_currency','value']) \
    .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

#display(currency_rates_df)

# COMMAND ----------

currency_rates_df.write.mode("overwrite").format("delta").saveAsTable("silver.currency_rates")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc EXTENDED silver.currency_rates
