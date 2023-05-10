# Databricks notebook source
# MAGIC %run "../../lib/configuration"

# COMMAND ----------

# MAGIC %run "../../lib/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat, sha2, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType

# COMMAND ----------

client_schema = StructType(fields = [
    StructField("id", IntegerType(), False),
    StructField("date_reg", TimestampType(), True),
    StructField("title_id", IntegerType(), True),
    StructField("birth_date", DateType(), True),
    StructField("state_reg_id", IntegerType(), True),
    StructField("city_reg", StringType(), True),
    StructField("change_status_ts", StringType(), True),
    StructField("status_id", IntegerType(), True),
    StructField("country_reg_id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("sex_id", IntegerType(), True),
    StructField("company_id", IntegerType(), True),
    StructField("reg_mail_is_send", IntegerType(), True),
    StructField("agent_id", IntegerType(), True),
    StructField("agent_source", StringType(), True),
    StructField("affiliate_id", StringType(), True),
    StructField("lang_id", IntegerType(), True),
    StructField("branch_id", IntegerType(), True),
    StructField("citizenship_country_id", IntegerType(), True),
    StructField("last_activity", TimestampType(), True),
    StructField("company_reg_number", StringType(), True),
    StructField("company_reg_city", StringType(), True),
    StructField("company_reg_country", IntegerType(), True),
    StructField("is_legal_form", IntegerType(), True),
    StructField("blacklist_id", IntegerType(), True),
    StructField("blacklist_bo_id", IntegerType(), True),
    StructField("automatic", IntegerType(), True),
    StructField("parent_id", IntegerType(), True),
    StructField("old_id", IntegerType(), True),
    StructField("old_table", StringType(), True),
    StructField("client_type_id", IntegerType(), True),
    StructField("created_boffice_user_id", IntegerType(), True),
    StructField("is_premium", IntegerType(), True),
    StructField("use_servers_rules", IntegerType(), True),
    StructField("strategy_explanation", StringType(), True),
    StructField("strategy_id", IntegerType(), True),
    StructField("strategy_ts", TimestampType(), True),
    StructField("pap_affiliate_id", StringType(), True),
    StructField("pap_ref_id", StringType(), True),
    StructField("pap_tmp_id", StringType(), True),
    StructField("ready_for_approval", IntegerType(), True),
    StructField("api", IntegerType(), True),
    StructField("promotions_restriction", IntegerType(), True),
    StructField("referrer_id", StringType(), True),
    StructField("is_raf_tc_accepted", IntegerType(), True),
    StructField("appropriateness_score", IntegerType(), True),
    StructField("appropriateness_type_id", IntegerType(), True),
    StructField("is_ngn", IntegerType(), True),
    StructField("nationality_country_id", IntegerType(), True),
    StructField("asm", StringType(), True),
    StructField("concat", StringType(), True),
    StructField("mifir", StringType(), True),
    StructField("due_diligence", IntegerType(), True),
    StructField("last_modify", TimestampType(), True),
    StructField("disable_mobile_trading", IntegerType(), True),
    StructField("last_trading_app_login_date", TimestampType(), True),
    StructField("appropriateness_maxscore_ts", IntegerType(), True),
    StructField("is_name_empty", IntegerType(), True),
    StructField("test", IntegerType(), True),
    StructField("agent_campaign_id", StringType(), True)
])

# COMMAND ----------

def func_client_sex(sex_id):
    if sex_id == 1:
        return "Male"
    if sex_id == 2:
        return "Female"
    else:
        return "Empty"

udf_client_sex = udf(func_client_sex, StringType())

# COMMAND ----------

client_df = spark.read \
    .schema(client_schema) \
    .parquet(f"{bronze_folder_path}/crm/client.parquet") \
    .dropDuplicates(['id']) \
    .withColumn("client_id", sha2(col('id').cast("string"), 256)) \
    .withColumn("client_sex", udf_client_sex(col('sex_id'))) \
    .withColumnRenamed("date_reg", "client_date_reg") \
    .withColumnRenamed("date_reg", "client_date_reg") \
    .withColumnRenamed("last_activity", "client_last_activity") \
    .select(['client_id', 'client_date_reg', 'birth_date', 'country_reg_id', 'client_sex', 'client_last_activity']) \
    .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

#display(client_df)

# COMMAND ----------

client_df.write.mode("overwrite").format("delta").saveAsTable("silver.client")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc EXTENDED silver.client
