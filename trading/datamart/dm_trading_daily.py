# Databricks notebook source
# MAGIC %run "../lib/configuration"

# COMMAND ----------

# MAGIC %run "../lib/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat, sha2, udf, from_unixtime, to_date, rand, round, count, sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType

# COMMAND ----------

from datetime import datetime
from dateutil.relativedelta import relativedelta

# COMMAND ----------

if not spark.catalog.tableExists("gold.dm_trading_daily"):
    #initial load
    p_start_date = '2023-01-01'
else:
    #update only last 2 months
    p_start_date = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-01')

p_end_date = (datetime.now() - relativedelta(days=1)).strftime('%Y-%m-%d') 

# COMMAND ----------

trading_df = spark.read.format("delta").load(f"{silver_folder_path}/trading") \
    .filter(f"trade_date BETWEEN '{p_start_date}' AND '{p_end_date}'") \
    .withColumnRenamed('symbol', 'instrument')

# COMMAND ----------

trading_account_df = spark.read.format("delta").load(f"{silver_folder_path}/trading_account") \
    .withColumnRenamed('client_id', 'account_client_id') \
    .select('frs_server_id','login','account_client_id', 'currency')

# COMMAND ----------

client_df = spark.read.format("delta").load(f"{silver_folder_path}/client") \
    .select('client_id', 'country_reg_id')

# COMMAND ----------

country_df = spark.read.format("delta").load(f"{silver_folder_path}/country") \
    .withColumnRenamed('country_name', 'country') \
    .select('country_id', 'country')

# COMMAND ----------

currency_rates_df = spark.read.format("delta").load(f"{silver_folder_path}/currency_rates") \
    .select('currency_date', 'src_currency', 'dst_currency', 'value')

# COMMAND ----------

result_df = trading_df \
    .join(trading_account_df, 
        [trading_account_df.frs_server_id == trading_df.frs_server_id, 
        trading_account_df.login == trading_df.login], 
        'inner') \
    .join(client_df, 
        [client_df.client_id == trading_account_df.account_client_id],
        'inner') \
    .join(country_df, 
        [country_df.country_id == client_df.country_reg_id],
        'left') \
    .join(currency_rates_df, 
        [currency_rates_df.currency_date == trading_df.trade_date,
        currency_rates_df.src_currency == trading_account_df.currency,
        currency_rates_df.dst_currency == 'USD'
        ],
        'left') \
    .withColumn('profit_usd', col('profit')*col('value')) \
    .withColumn('swap_usd', col('swap')*col('value')) \
    .withColumn('commission_usd', col('commission')*col('value')) \
    .withColumn('realized_pl_usd', col('profit_usd')+col('swap_usd')+col('commission_usd'))

# COMMAND ----------

result_agg_df = result_df \
    .groupBy('trade_date', "client_id", "country", "instrument") \
    .agg(
        sum("profit_usd").alias("profit_usd")
        ,sum("swap_usd").alias("swap_usd")
        ,sum("commission_usd").alias("commission_usd")
        ,sum("realized_pl_usd").alias("realized_pl_usd")
        ,count(lit(1)).alias("deal_count")
        ) \
    .withColumn("created_date", current_timestamp())

# COMMAND ----------

merge_condition = "src.trade_date = tgt.trade_date AND src.client_id = tgt.client_id AND src.instrument = tgt.instrument"
merge_delta_data(result_agg_df, "gold", "dm_trading_daily", "trade_date", gold_folder_path, merge_condition)

# COMMAND ----------

dbutils.notebook.exit(f"Success - Updated {p_start_date} - {p_end_date}")

# COMMAND ----------

!!!!! check DUPLICATES

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(*) cnt
# MAGIC from gold.dm_trading_daily
# MAGIC where 
# MAGIC   trade_date = '2023-05-04'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY gold.dm_trading_daily

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM gold.dm_trading_daily
# MAGIC WHERE
# MAGIC   trade_date = '2023-05-04'
# MAGIC   AND country != 'EGYPT'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(*) cnt
# MAGIC from gold.dm_trading_daily
# MAGIC where 
# MAGIC   trade_date = '2023-05-04'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(*) cnt
# MAGIC from gold.dm_trading_daily VERSION AS OF 1
# MAGIC where 
# MAGIC   trade_date = '2023-05-04'

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE gold.dm_trading_daily TO VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(*) cnt
# MAGIC from gold.dm_trading_daily
# MAGIC where 
# MAGIC   trade_date = '2023-05-04'

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM gold.dm_trading_daily RETAIN 0 HOURS
# MAGIC --VACUUM gold.dm_trading_daily RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY gold.dm_trading_daily

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from gold.dm_trading_daily VERSION AS OF 2
# MAGIC where trade_date = '2023-05-04'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) cnt
# MAGIC from gold.dm_trading_daily VERSION AS OF 2
# MAGIC where trade_date = '2023-05-04'
