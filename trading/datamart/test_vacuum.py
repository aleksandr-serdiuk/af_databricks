# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS udemy_test.test_table;
# MAGIC CREATE TABLE IF NOT EXISTS udemy_test.test_table
# MAGIC (
# MAGIC   id INT,
# MAGIC   value STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO udemy_test.test_table VALUES (1, 'test1'), (2, 'test2'), (3, 'test3')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY udemy_test.test_table 

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM udemy_test.test_table
# MAGIC WHERE id = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM udemy_test.test_table VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC --SET spark.databricks.delta.vacuum.logging.enabled = true;
# MAGIC --VACUUM udemy_test.test_table RETAIN 0 HOURS DRY RUN
# MAGIC VACUUM udemy_test.test_table RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM udemy_test.test_table VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY udemy_test.test_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from udemy_test.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from udemy_test.drivers_merge
# MAGIC where driverId > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY udemy_test.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from udemy_test.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from udemy_test.drivers_merge VERSION AS OF 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC --SET spark.databricks.delta.vacuum.logging.enabled = true;
# MAGIC --VACUUM udemy_test.test_table RETAIN 0 HOURS DRY RUN
# MAGIC VACUUM udemy_test.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from udemy_test.drivers_merge VERSION AS OF 10
