# Databricks notebook source
# MAGIC %run "../../lib/configuration"

# COMMAND ----------

# MAGIC %run "../../lib/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat, sha2, udf, from_unixtime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType

# COMMAND ----------

trading_account_mt4_df = spark.read \
    .option("inferSchema", True) \
    .parquet(f"{bronze_folder_path}/dict/mt4_user_record.parquet") \
    .filter("frs_RecOperation IN ('I', 'U', 'X')") \
    .withColumn("platform", lit("mt4")) \
    .withColumn("login", sha2(col('login').cast("string"), 256)) \
    .withColumn("login_group", sha2(col('group'), 256)) \
    .withColumn("client_id", sha2(col('ClientID'), 256)) \
    .withColumn("registration_ts", from_unixtime(col('regdate'))) \
    .withColumnRenamed("frs_ServerID", "frs_server_id") \
    .select(['platform', 'frs_server_id', 'login', 'login_group', 'client_id', 'registration_ts'])

# COMMAND ----------

trading_account_group_mt4_df = spark.read \
    .option("inferSchema", True) \
    .parquet(f"{bronze_folder_path}/dict/mt4_con_group.parquet") \
    .filter("frs_RecOperation IN ('I', 'U', 'X')") \
    .withColumnRenamed("frs_ServerID", "ag_frs_server_id") \
    .withColumn("ag_group", sha2(col('group'), 256)) \
    .select(['ag_frs_server_id', 'ag_group', 'currency'])

# COMMAND ----------

ta_mt4_df = trading_account_mt4_df \
    .join(trading_account_group_mt4_df, [trading_account_mt4_df.frs_server_id == trading_account_group_mt4_df.ag_frs_server_id, trading_account_mt4_df.login_group == trading_account_group_mt4_df.ag_group], 'left') \
    .select(['platform', 'frs_server_id', 'login', 'login_group', 'client_id', 'registration_ts', 'currency'])

# COMMAND ----------

trading_account_mt5_df = spark.read \
    .option("inferSchema", True) \
    .parquet(f"{bronze_folder_path}/dict/mt5_user.parquet") \
    .filter("frs_RecOperation IN ('I', 'U', 'X')") \
    .withColumn("platform", lit("mt5")) \
    .withColumn("login", sha2(col('Login').cast("string"), 256)) \
    .withColumn("login_group", sha2(col('Group'), 256)) \
    .withColumn("client_id", sha2(col('ClientID'), 256)) \
    .withColumn("registration_ts", from_unixtime(col('Registration'))) \
    .withColumnRenamed("frs_ServerID", "frs_server_id") \
    .select(['platform', 'frs_server_id', 'login', 'login_group', 'client_id', 'registration_ts'])

# COMMAND ----------

trading_account_group_mt5_df = spark.read \
    .option("inferSchema", True) \
    .parquet(f"{bronze_folder_path}/dict/mt5_con_group.parquet") \
    .filter("frs_RecOperation IN ('I', 'U', 'X')") \
    .withColumnRenamed("frs_ServerID", "ag_frs_server_id") \
    .withColumn("ag_group", sha2(col('group'), 256)) \
    .select(['ag_frs_server_id', 'ag_group', 'currency'])

# COMMAND ----------

ta_mt5_df = trading_account_mt5_df \
    .join(trading_account_group_mt5_df, [trading_account_mt5_df.frs_server_id == trading_account_group_mt5_df.ag_frs_server_id, trading_account_mt5_df.login_group == trading_account_group_mt5_df.ag_group], 'left') \
    .select(['platform', 'frs_server_id', 'login', 'login_group', 'client_id', 'registration_ts', 'currency'])

# COMMAND ----------

trading_account_df = ta_mt4_df.union(ta_mt5_df) \
    .dropDuplicates(['frs_server_id', 'login']) \
    .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

#display(trading_account_df)

# COMMAND ----------

trading_account_df.write.mode("overwrite").format("delta").saveAsTable("silver.trading_account")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.trading_account
