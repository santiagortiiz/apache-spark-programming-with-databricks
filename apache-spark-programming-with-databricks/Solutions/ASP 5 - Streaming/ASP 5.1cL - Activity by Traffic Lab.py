# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-7490cd7f-a3a1-41ac-8fa8-b126983ae4f5
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Activity by Traffic Lab
# MAGIC Process streaming data to display total active users by traffic source.
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Read data stream
# MAGIC 2. Get active users by traffic source
# MAGIC 3. Execute query with display() and plot results
# MAGIC 4. Execute the same streaming query with DataStreamWriter
# MAGIC 5. View results being updated in the query table
# MAGIC 6. List and stop all active streams
# MAGIC
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>

# COMMAND ----------

# DBTITLE 0,--i18n-ffb260b5-eef7-435a-94da-f43d123719f2
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Setup
# MAGIC Run the cells below to generate data and create the **`schema`** string needed for this lab.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-5.1c

# COMMAND ----------

# DBTITLE 0,--i18n-5cc77c5c-04f9-4306-a0f2-a8848bf79e60
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Read data stream
# MAGIC - Set to process 1 file per trigger
# MAGIC - Read from Delta with filepath stored in **`DA.paths.events`**
# MAGIC
# MAGIC Assign the resulting Query to **`df`**.

# COMMAND ----------

# ANSWER
df = (spark.readStream
           .option("maxFilesPerTrigger", 1)
           .format("delta")
           .load(DA.paths.events))

# COMMAND ----------

# DBTITLE 0,--i18n-d1dbded3-bf92-4f51-882f-ff7354079ca4
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

DA.tests.validate_1_1(df)

# COMMAND ----------

# DBTITLE 0,--i18n-a6dada0b-e17f-4491-8b55-de63101d60c4
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Get active users by traffic source
# MAGIC - Set default shuffle partitions to number of cores on your cluster (not required, but runs faster)
# MAGIC - Group by **`traffic_source`**
# MAGIC   - Aggregate the approximate count of distinct users and alias with "active_users"
# MAGIC - Sort by **`traffic_source`**

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col, approx_count_distinct, count

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

traffic_df = (df
              .groupBy("traffic_source")
              .agg(approx_count_distinct("user_id").alias("active_users"))
              .sort("traffic_source")
             )

# COMMAND ----------

# DBTITLE 0,--i18n-b202f51b-7b0c-48bc-a705-fd43b08abd6b
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

DA.tests.validate_2_1(traffic_df.schema)

# COMMAND ----------

# DBTITLE 0,--i18n-048e3d16-65a1-4502-b76a-b5605b454df7
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 3. Execute query with display() and plot results
# MAGIC - Execute results for **`traffic_df`** using display()
# MAGIC - Plot the streaming query results as a bar graph

# COMMAND ----------

# ANSWER
display(traffic_df)

# COMMAND ----------

# DBTITLE 0,--i18n-7ba01b58-dfa9-4554-9d33-94edb188a718
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**
# MAGIC - You bar chart should plot **`traffic_source`** on the x-axis and **`active_users`** on the y-axis
# MAGIC - The top three traffic sources in descending order should be **`google`**, **`facebook`**, and **`instagram`**.

# COMMAND ----------

# DBTITLE 0,--i18n-d5695c83-1fc1-478d-8319-ee6a57f16793
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 4. Execute the same streaming query with DataStreamWriter
# MAGIC - Name the query "active_users_by_traffic"
# MAGIC - Set to "memory" format and "complete" output mode
# MAGIC - Set a trigger interval of 1 second

# COMMAND ----------

# ANSWER
traffic_query = (traffic_df
                 .writeStream
                 .queryName("active_users_by_traffic")
                 .format("memory")
                 .outputMode("complete")
                 .trigger(processingTime="1 second")
                 .start())

DA.block_until_stream_is_ready("active_users_by_traffic")

# COMMAND ----------

# DBTITLE 0,--i18n-ff33fa6a-26e6-48eb-be38-4a035fe15829
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

DA.tests.validate_4_1(traffic_query)

# COMMAND ----------

# DBTITLE 0,--i18n-5560d8c5-5acb-477d-ab0c-c7403560cf25
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 5. View results being updated in the query table
# MAGIC Run a query in a SQL cell to display the results from the **`active_users_by_traffic`** table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT * FROM active_users_by_traffic

# COMMAND ----------

# DBTITLE 0,--i18n-2c5cd18b-39f8-40fc-9d3b-9d313034ec10
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **5.1: CHECK YOUR WORK**
# MAGIC Your query should eventually result in the following values.
# MAGIC
# MAGIC |traffic_source|active_users|
# MAGIC |---|---|
# MAGIC |direct|438886|
# MAGIC |email|281525|
# MAGIC |facebook|956769|
# MAGIC |google|1781961|
# MAGIC |instagram|530050|
# MAGIC |youtube|253321|

# COMMAND ----------

# DBTITLE 0,--i18n-efd51cf5-f848-4440-9ab7-ebe2650922ae
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 6. List and stop all active streams
# MAGIC - Use SparkSession to get list of all active streams
# MAGIC - Iterate over the list and stop each query

# COMMAND ----------

# ANSWER
for s in spark.streams.active:
    print(s.name)
    s.stop()

# COMMAND ----------

# DBTITLE 0,--i18n-3d8b9236-eb09-415a-976e-4098c513043c
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **6.1: CHECK YOUR WORK**

# COMMAND ----------

DA.tests.validate_6_1(traffic_query)

# COMMAND ----------

# DBTITLE 0,--i18n-778b92a6-160a-4210-8e9d-c49ef0621fb7
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
