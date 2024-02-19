# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-c22511ee-b15c-4a9f-a051-c905b8e344e9
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Coupon Sales Lab
# MAGIC Process and append streaming data on transactions using coupons.
# MAGIC 1. Read data stream
# MAGIC 2. Filter for transactions with coupons codes
# MAGIC 3. Write streaming query results to Delta
# MAGIC 4. Monitor streaming query
# MAGIC 5. Stop streaming query
# MAGIC
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-5.1a

# COMMAND ----------

# DBTITLE 0,--i18n-d166abd5-a348-46f9-8134-72f510207bc1
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Read data stream
# MAGIC - Set to process 1 file per trigger
# MAGIC - Read from Delta files in the source directory specified by **`DA.paths.sales`**
# MAGIC
# MAGIC Assign the resulting DataFrame to **`df`**.

# COMMAND ----------

# TODO
df = (spark.FILL_IN
)

# COMMAND ----------

# DBTITLE 0,--i18n-7655ea48-abe2-4ff2-abf3-3d25e728fbe3
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

DA.tests.validate_1_1(df)

# COMMAND ----------

# DBTITLE 0,--i18n-d3f81153-b55d-4ce8-9012-91fd09185880
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Filter for transactions with coupon codes
# MAGIC - Explode the **`items`** field in **`df`** with the results replacing the existing **`items`** field
# MAGIC - Filter for records where **`items.coupon`** is not null
# MAGIC
# MAGIC Assign the resulting DataFrame to **`coupon_sales_df`**.

# COMMAND ----------

# TODO
coupon_sales_df = (df.FILL_IN
)

# COMMAND ----------

# DBTITLE 0,--i18n-37e4766a-e845-4b4e-9e7e-82d63fbfc29f
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

DA.tests.validate_2_1(coupon_sales_df.schema)

# COMMAND ----------

# DBTITLE 0,--i18n-793d6d02-ac0b-4dfa-b711-b4a01636e099
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 3. Write streaming query results to Delta
# MAGIC - Configure the streaming query to write Delta format files in "append" mode
# MAGIC - Set the query name to "coupon_sales"
# MAGIC - Set a trigger interval of 1 second
# MAGIC - Set the checkpoint location to **`coupons_checkpoint_path`**
# MAGIC - Set the output path to **`coupons_output_path`**
# MAGIC
# MAGIC Start the streaming query and assign the resulting handle to **`coupon_sales_query`**.

# COMMAND ----------

# TODO
coupons_checkpoint_path = f"{DA.paths.checkpoints}/coupon-sales"
coupons_output_path = f"{DA.paths.working_dir}/coupon-sales/output"

coupon_sales_query = (coupon_sales_df.FILL_IN)

DA.block_until_stream_is_ready(coupon_sales_query)

# COMMAND ----------

# DBTITLE 0,--i18n-204ee663-db1b-441a-9084-a58a9aa88a9c
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**

# COMMAND ----------

DA.tests.validate_3_1(coupon_sales_query)

# COMMAND ----------

# DBTITLE 0,--i18n-a8c6095a-6725-4ecc-8c23-f85832d548f9
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 4. Monitor streaming query
# MAGIC - Get the ID of streaming query and store it in **`queryID`**
# MAGIC - Get the status of streaming query and store it in **`queryStatus`**

# COMMAND ----------

# TODO
query_id = coupon_sales_query.FILL_IN

# COMMAND ----------

# TODO
query_status = coupon_sales_query.FILL_IN

# COMMAND ----------

# DBTITLE 0,--i18n-80d700dc-c312-4c0d-8997-1ce7fe69aefd
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

DA.tests.validate_4_1(query_id, query_status)

# COMMAND ----------

# DBTITLE 0,--i18n-fac79dfb-738f-4c44-ae25-8e9439866be6
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 5. Stop streaming query
# MAGIC - Stop the streaming query

# COMMAND ----------

# TODO
coupon_sales_query.FILL_IN

# COMMAND ----------

# DBTITLE 0,--i18n-4b582543-cdec-47f0-9ee3-55a8b2dc42db
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **5.1: CHECK YOUR WORK**

# COMMAND ----------

DA.tests.validate_5_1(coupon_sales_query)

# COMMAND ----------

# DBTITLE 0,--i18n-9e759d24-b277-4237-86ff-92650fdac8f7
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 6. Verify the records were written in Delta format

# COMMAND ----------

# TODO

# COMMAND ----------

# DBTITLE 0,--i18n-7917bc49-1f8c-404a-b37c-76fa3d945620
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Classroom Cleanup
# MAGIC Run the cell below to clean up resources.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
