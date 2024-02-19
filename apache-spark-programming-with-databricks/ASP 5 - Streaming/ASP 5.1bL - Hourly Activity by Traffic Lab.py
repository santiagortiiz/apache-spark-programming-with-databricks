# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-5d844227-898e-41b2-adf0-bdc282c32a19
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Hourly Activity by Traffic Lab
# MAGIC Process streaming data to display the total active users by traffic source with a 1 hour window.
# MAGIC 1. Cast to timestamp and add watermark for 2 hours
# MAGIC 2. Aggregate active users by traffic source for 1 hour windows
# MAGIC 3. Execute query with **`display`** and plot results
# MAGIC 5. Use query name to stop streaming query

# COMMAND ----------

# DBTITLE 0,--i18n-734c8d4c-5d28-4370-9403-1531ae27fd3c
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Setup
# MAGIC Run the cells below to generate hourly JSON files of event data for July 3, 2020.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-5.1b

# COMMAND ----------

schema = "device STRING, ecommerce STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name STRING, event_previous_timestamp BIGINT, event_timestamp BIGINT, geo STRUCT<city: STRING, state: STRING>, items ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source STRING, user_first_touch_timestamp BIGINT, user_id STRING"

# Directory of hourly events logged from the BedBricks website on July 3, 2020
hourly_events_path = f"{DA.paths.datasets}/ecommerce/events/events-2020-07-03.json"

df = (spark.readStream
           .schema(schema)
           .option("maxFilesPerTrigger", 1)
           .json(hourly_events_path))

# COMMAND ----------

# DBTITLE 0,--i18n-99264197-aba0-4df5-8346-8501cd6efba0
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Cast to timestamp and add watermark for 2 hours
# MAGIC - Add a **`createdAt`** column by dividing **`event_timestamp`** by 1M and casting to timestamp
# MAGIC - Set a watermark of 2 hours on the **`createdAt`** column
# MAGIC
# MAGIC Assign the resulting DataFrame to **`events_df`**.

# COMMAND ----------

# TODO
events_df = (df.FILL_IN)

# COMMAND ----------

# DBTITLE 0,--i18n-73d176cb-bf2f-450b-bdaf-a72427e0947d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

DA.tests.validate_1_1(events_df.schema)

# COMMAND ----------

# DBTITLE 0,--i18n-db2b1906-b389-4f49-889c-c9474d381a3f
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Aggregate active users by traffic source for 1 hour windows
# MAGIC
# MAGIC - Set the default shuffle partitions to the number of cores on your cluster
# MAGIC - Group by **`traffic_source`** with 1-hour tumbling windows based on the **`createdAt`** column
# MAGIC - Aggregate the approximate count of distinct users per **`user_id`** and alias the resulting column to **`active_users`**
# MAGIC - Select **`traffic_source`**, **`active_users`**, and the **`hour`** extracted from **`window.start`** with an alias of **`hour`**
# MAGIC - Sort by **`hour`** in ascending order
# MAGIC Assign the resulting DataFrame to **`traffic_df`**.

# COMMAND ----------

# TODO
spark.FILL_IN

traffic_df = (events_df.FILL_IN
)

# COMMAND ----------

# DBTITLE 0,--i18n-2a1ee964-474d-450f-99e2-9c023406fa73
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

DA.tests.validate_2_1(traffic_df.schema)

# COMMAND ----------

# DBTITLE 0,--i18n-690240f6-4981-405c-8719-2f24054c889d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 3. Execute query with display() and plot results
# MAGIC - Use **`display`** to start **`traffic_df`** as a streaming query and display the resulting memory sink
# MAGIC   - Assign "hourly_traffic" as the name of the query by setting the **`streamName`** parameter of **`display`**
# MAGIC - Plot the streaming query results as a bar graph
# MAGIC - Configure the following plot options:
# MAGIC   - Keys: **`hour`**
# MAGIC   - Series groupings: **`traffic_source`**
# MAGIC   - Values: **`active_users`**

# COMMAND ----------

# TODO

# COMMAND ----------

# DBTITLE 0,--i18n-80df0c45-6643-4933-816a-3c63b68719a0
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**
# MAGIC
# MAGIC - The bar chart should plot **`hour`** on the x-axis and **`active_users`** on the y-axis
# MAGIC - Six bars should appear at every hour for all traffic sources
# MAGIC - The chart should stop at hour 23

# COMMAND ----------

# DBTITLE 0,--i18n-f9ef0d57-9b71-4bfa-ae83-b854770c603d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 4. Manage streaming query
# MAGIC - Iterate over SparkSession's list of active streams to find one with name "hourly_traffic"
# MAGIC - Stop the streaming query

# COMMAND ----------

# TODO
DA.block_until_stream_is_ready("hourly_traffic")

for s in FILL_IN:

# COMMAND ----------

# DBTITLE 0,--i18n-9ecfd9e1-edfd-4964-b45e-fa207f194320
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**
# MAGIC Print all active streams to check that "hourly_traffic" is no longer there

# COMMAND ----------

DA.tests.validate_4_1()

# COMMAND ----------

# DBTITLE 0,--i18n-a78e18c1-6071-4759-9e57-61060aab83bc
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
