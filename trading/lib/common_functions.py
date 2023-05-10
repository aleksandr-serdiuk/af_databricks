# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def rearange_partition_column(input_df, partition_column):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)

    return input_df.select(column_list)

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    input_df = rearange_partition_column(input_df, partition_column)

    #to overwrite partition
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if(spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        input_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

from delta.tables import DeltaTable

def merge_delta_data(input_df, db_name, table_name, partition_column, folder_path, merge_condition):
    #to overwrite partition
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if(spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        DeltaTable.forPath(spark, f"{folder_path}/{table_name}") \
            .alias("tgt") \
            .merge(
                input_df.alias("src"),
                merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
