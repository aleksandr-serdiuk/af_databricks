# Databricks notebook source
# MAGIC %run "../../lib/configuration"

# COMMAND ----------

# MAGIC %run "../../lib/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2023-02-01")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat, sha2, udf, from_unixtime, to_date, rand, round
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType

# COMMAND ----------

trading_open_mt4_df = spark.read \
    .option("inferSchema", True) \
    .parquet(f"{bronze_folder_path}/trade/{v_file_date}/mt4_trade_record.parquet") \
    .filter("frs_RecOperation IN ('I', 'U', 'X') AND cmd IN (0, 1)") \
    .withColumn("platform", lit("mt4")) \
    .withColumn("login", sha2(col('login').cast("string"), 256)) \
    .withColumn("trade_ts", from_unixtime(col('open_time'))) \
    .withColumn("entry", lit(0)) \
    .withColumn("symbol", sha2(col('symbol').cast("string"), 256)) \
    .withColumn("profit", lit(0)) \
    .withColumn("swap", lit(0)) \
    .withColumn("commission", lit(0)) \
    .withColumn("volume", col('volume')*round(rand()*100)) \
    .withColumnRenamed("frs_ServerID", "frs_server_id") \
    .withColumnRenamed("order", "deal_id") \
    .withColumnRenamed("cmd", "action") \
    .withColumnRenamed("open_price", "price") \
    .select(['platform', 'frs_server_id', 'login', 'deal_id', 'entry', 'action', 'symbol', 'trade_ts', 'price', 'volume', 'profit', 'swap', 'commission'])

# COMMAND ----------

def func_mt4_action(cmd):
    if cmd == 0:
        return 1
    else:
        return 0

udf_mt4_action = udf(func_mt4_action, IntegerType())

# COMMAND ----------

trading_close_mt4_df = spark.read \
    .option("inferSchema", True) \
    .parquet(f"{bronze_folder_path}/trade/{v_file_date}/mt4_trade_record.parquet") \
    .filter("frs_RecOperation IN ('I', 'U', 'X') AND cmd IN (0, 1) AND close_time > 0") \
    .withColumn("platform", lit("mt4")) \
    .withColumn("login", sha2(col('login').cast("string"), 256)) \
    .withColumn("trade_ts", from_unixtime(col('close_time'))) \
    .withColumn("entry", lit(1)) \
    .withColumn("symbol", sha2(col('symbol').cast("string"), 256)) \
    .withColumn("action", udf_mt4_action(col('cmd'))) \
    .withColumn("profit", col('profit')*round(rand()*100)) \
    .withColumn("swap", col('storage')*round(rand()*100)) \
    .withColumn("commission", col('commission')*round(rand()*100)) \
    .withColumn("volume", col('volume')*round(rand()*100)) \
    .withColumnRenamed("frs_ServerID", "frs_server_id") \
    .withColumnRenamed("order", "deal_id") \
    .withColumnRenamed("open_price", "price") \
    .select(['platform', 'frs_server_id', 'login', 'deal_id', 'entry', 'action', 'symbol', 'trade_ts', 'price', 'volume', 'profit', 'swap', 'commission'])

# COMMAND ----------

trading_mt5_df = spark.read \
    .option("inferSchema", True) \
    .parquet(f"{bronze_folder_path}/trade/{v_file_date}/mt5_deal.parquet") \
    .filter("frs_RecOperation IN ('I', 'U', 'X') AND Action IN (0, 1)") \
    .withColumn("platform", lit("mt5")) \
    .withColumn("login", sha2(col('Login').cast("string"), 256)) \
    .withColumn("trade_ts", from_unixtime(col('Time'))) \
    .withColumn("symbol", sha2(col('Symbol').cast("string"), 256)) \
    .withColumn("profit", col('profit')*round(rand()*100)) \
    .withColumn("swap", col('storage')*round(rand()*100)) \
    .withColumn("commission", col('commission')*round(rand()*100)) \
    .withColumn("volume", col('volume')*round(rand()*100)) \
    .withColumnRenamed("frs_ServerID", "frs_server_id") \
    .withColumnRenamed("Entry", "entry") \
    .withColumnRenamed("Action", "action") \
    .withColumnRenamed("order", "deal_id") \
    .withColumnRenamed("open_price", "price") \
    .select(['platform', 'frs_server_id', 'login', 'deal_id', 'entry', 'action', 'symbol', 'trade_ts', 'price', 'volume', 'profit', 'swap', 'commission'])

# COMMAND ----------

trading_df = trading_open_mt4_df \
    .union(trading_close_mt4_df) \
    .union(trading_mt5_df) \
    .dropDuplicates(['frs_server_id', 'deal_id', 'entry']) \
    .withColumn("trade_date", to_date(col('trade_ts'))) \
    .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

#display(trading_df)

# COMMAND ----------

merge_condition = "src.deal_id = tgt.deal_id AND src.frs_server_id = tgt.frs_server_id AND src.entry = tgt.entry AND src.trade_date = tgt.trade_date"
merge_delta_data(trading_df, "silver", "trading", "trade_date", silver_folder_path, merge_condition)

# COMMAND ----------

dbutils.notebook.exit(f"Success - Updated {v_file_date}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.trading
