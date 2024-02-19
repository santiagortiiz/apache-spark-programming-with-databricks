# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-b7ab8d00-d772-4194-be2b-21869a88d017
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Delta Lake Lab
# MAGIC ##### Tasks
# MAGIC 1. Write sales data to Delta
# MAGIC 1. Modify sales data to show item count instead of item array
# MAGIC 1. Rewrite sales data to same Delta path
# MAGIC 1. Create table and view version history
# MAGIC 1. Time travel to read previous version

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

sales_df = spark.read.parquet(f"{DA.paths.datasets}/ecommerce/sales/sales.parquet")
delta_sales_path = f"{DA.paths.working_dir}/delta-sales"

# COMMAND ----------

# DBTITLE 0,--i18n-3ddef38d-a094-4485-891b-04aba30c1fc3
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 1. Write sales data to Delta
# MAGIC Write **`sales_df`** to **`delta_sales_path`**

# COMMAND ----------

# ANSWER
sales_df.write.format("delta").mode("overwrite").save(delta_sales_path)

# COMMAND ----------

# DBTITLE 0,--i18n-b23c5a11-5446-48b8-9e9e-d3aa051473fe
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

assert len(dbutils.fs.ls(delta_sales_path)) > 0

# COMMAND ----------

# DBTITLE 0,--i18n-a7ff98a7-66d3-49a8-a3d3-6d1f12e329d6
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Modify sales data to show item count instead of item array
# MAGIC Replace values in the **`items`** column with an integer value of the items array size.
# MAGIC Assign the resulting DataFrame to **`updated_sales_df`**.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import size, col

updated_sales_df = sales_df.withColumn("items", size(col("items")))
display(updated_sales_df)

# COMMAND ----------

# DBTITLE 0,--i18n-a6058b29-1241-4067-9c31-e716d04c9840
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.types import IntegerType

assert updated_sales_df.schema[6].dataType == IntegerType()
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-1449e3a9-db22-49e7-8b0b-b1b0c2a50a25
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 3. Rewrite sales data to same Delta path
# MAGIC Write **`updated_sales_df`** to the same Delta location **`delta_sales_path`**.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> This will fail without an option to overwrite the schema.

# COMMAND ----------

# ANSWER
updated_sales_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_sales_path)

# COMMAND ----------

# DBTITLE 0,--i18n-56b5c3af-c575-4a44-becb-63b0a5321872
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**

# COMMAND ----------

assert spark.read.format("delta").load(delta_sales_path).schema[6].dataType == IntegerType()
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-646700b2-7030-4adc-858e-3866c4917923
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 4. Create table and view version history
# MAGIC Run SQL queries by writing SQL inside of `spark.sql()` to perform the following steps.
# MAGIC - Drop table **`sales_delta`** if it exists
# MAGIC - Create **`sales_delta`** table using the **`delta_sales_path`** location
# MAGIC - List version history for the **`sales_delta`** table
# MAGIC
# MAGIC An example of a SQL query inside of `spark.sql()` would be something like ```spark.sql("SELECT * FROM sales_data")```

# COMMAND ----------

# ANSWER
spark.sql("DROP TABLE IF EXISTS sales_delta")
spark.sql("CREATE TABLE sales_delta USING DELTA LOCATION '{}'".format(delta_sales_path))

# COMMAND ----------

# ANSWER
display(spark.sql("DESCRIBE HISTORY sales_delta"))

# COMMAND ----------

# DBTITLE 0,--i18n-ba038ee5-a6ff-4744-9876-3c2b088f3ca4
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

sales_delta_df = spark.sql("SELECT * FROM sales_delta")
assert sales_delta_df.count() == 210370
assert sales_delta_df.schema[6].dataType == IntegerType()
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-0d7572b5-2bd5-4f4b-a42a-4810bd7fca92
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 5. Time travel to read previous version
# MAGIC Read delta table at **`delta_sales_path`** at version 0.
# MAGIC Assign the resulting DataFrame to **`old_sales_df`**.

# COMMAND ----------

# ANSWER
old_sales_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_sales_path)
display(old_sales_df)

# COMMAND ----------

# DBTITLE 0,--i18n-c36128f3-0818-4dc4-a628-a35f97d253ba
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **5.1: CHECK YOUR WORK**

# COMMAND ----------

assert old_sales_df.select(size(col("items"))).first()[0] == 1
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-3e03f968-f1f4-40d2-903f-fab13b238a52
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
