# Databricks notebook source
# MAGIC %run "../../lib/configuration"

# COMMAND ----------

# MAGIC %run "../../lib/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat, sha2, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType

# COMMAND ----------

def func_currency_profit(profit_mode, symbol, currency):
    if profit_mode == 0:
        return symbol[3:6]
    if profit_mode == 1:
        return currency[:3]
    else:
        return currency

udf_currency_profit = udf(func_currency_profit, StringType())

# COMMAND ----------

trading_instrument_mt4_df = spark.read \
    .option("inferSchema", True) \
    .parquet(f"{bronze_folder_path}/dict/mt4_con_symbol.parquet") \
    .filter("frs_RecOperation IN ('I', 'U', 'X')") \
    .withColumn("platform", lit("mt4")) \
    .withColumn("currency_profit", udf_currency_profit(col('profit_mode'),col('symbol'),col('currency'))) \
    .withColumn("symbol", sha2(col('symbol'), 256)) \
    .withColumnRenamed("frs_ServerID", "frs_server_id") \
    .withColumnRenamed("currency", "currency_base") \
    .select(['platform', 'frs_server_id', 'symbol', 'currency_base', 'currency_profit', 'profit_mode', 'type'])

# COMMAND ----------

trading_instrument_group_mt4_df = spark.read \
    .option("inferSchema", True) \
    .parquet(f"{bronze_folder_path}/dict/mt4_con_symbol_group.parquet") \
    .filter("frs_RecOperation IN ('I', 'U', 'X')") \
    .withColumnRenamed("frs_ServerID", "sg_frs_server_id") \
    .withColumnRenamed("index", "type") \
    .withColumn("symbol_group", sha2(col('name'), 256)) \
    .select(['sg_frs_server_id', 'type', 'symbol_group'])

# COMMAND ----------

ti_mt4_df = trading_instrument_mt4_df \
    .join(trading_instrument_group_mt4_df, [trading_instrument_mt4_df.frs_server_id == trading_instrument_group_mt4_df.sg_frs_server_id, trading_instrument_mt4_df.type == trading_instrument_group_mt4_df.type], 'left') \
    .select(['platform', 'frs_server_id', 'symbol', 'symbol_group', 'currency_base', 'currency_profit', 'profit_mode'])

# COMMAND ----------

trading_instrument_mt5_df = spark.read \
    .option("inferSchema", True) \
    .parquet(f"{bronze_folder_path}/dict/mt5_con_symbol.parquet") \
    .filter("frs_RecOperation IN ('I', 'U', 'X')") \
    .withColumn("platform", lit("mt5")) \
    .withColumn("symbol", sha2(col('Symbol'), 256)) \
    .withColumn("symbol_group", sha2(col('Path'), 256)) \
    .withColumn("profit_mode", lit(0)) \
    .withColumnRenamed("frs_ServerID", "frs_server_id") \
    .withColumnRenamed("CurrencyBase", "currency_base") \
    .withColumnRenamed("CurrencyProfit", "currency_profit") \
    .select(['platform', 'frs_server_id', 'symbol', 'symbol_group', 'currency_base', 'currency_profit', 'profit_mode'])

# COMMAND ----------

trading_instrument_df = ti_mt4_df.union(trading_instrument_mt5_df) \
    .dropDuplicates(['frs_server_id', 'symbol']) \
    .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

#display(trading_instrument_df)

# COMMAND ----------

trading_instrument_df.write.mode("overwrite").format("delta").saveAsTable("silver.trading_instrument")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.trading_instrument
