# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-c27a0f32-d70b-4289-b9cb-0b3f57d6a883
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Streaming Query
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Build streaming DataFrames
# MAGIC 1. Display streaming query results
# MAGIC 1. Write streaming query results
# MAGIC 1. Monitor streaming query
# MAGIC
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# DBTITLE 0,--i18n-d483091a-0eff-4095-b07c-b7616d03e03f
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Build streaming DataFrames
# MAGIC
# MAGIC Obtain an initial streaming DataFrame from a Delta-format file source.

# COMMAND ----------

df = (spark
      .readStream
      .option("maxFilesPerTrigger", 1)
      .format("delta")
      .load(DA.paths.events)
     )

df.isStreaming

# COMMAND ----------

# DBTITLE 0,--i18n-71d3e0bc-7405-4115-8603-467e56dd9c9e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Apply some transformations, producing new streaming DataFrames.

# COMMAND ----------

from pyspark.sql.functions import col, approx_count_distinct, count

email_traffic_df = (df
                    .filter(col("traffic_source") == "email")
                    .withColumn("mobile", col("device").isin(["iOS", "Android"]))
                    .select("user_id", "event_timestamp", "mobile")
                   )

email_traffic_df.isStreaming

# COMMAND ----------

# DBTITLE 0,--i18n-2afbce6a-e773-49c9-b693-95db2257ead2
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Write streaming query results
# MAGIC
# MAGIC Take the final streaming DataFrame (our result table) and write it to a file sink in "append" mode.

# COMMAND ----------

checkpoint_path = f"{DA.paths.checkpoints}/email_traffic"
output_path = f"{DA.paths.working_dir}/email_traffic/output"

devices_query = (email_traffic_df
                 .writeStream
                 .outputMode("append")
                 .format("delta")
                 .queryName("email_traffic")
                 .trigger(processingTime="1 second")
                 .option("checkpointLocation", checkpoint_path)
                 .start(output_path)
                )

# COMMAND ----------

# DBTITLE 0,--i18n-abebb6ba-cc8b-49a5-8976-07aa5736f81b
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Monitor streaming query
# MAGIC
# MAGIC Use the streaming query "handle" to monitor and control it.

# COMMAND ----------

devices_query.id

# COMMAND ----------

devices_query.status

# COMMAND ----------

devices_query.lastProgress

# COMMAND ----------

import time
# Run for 10 more seconds
time.sleep(10) 

devices_query.stop()

# COMMAND ----------

devices_query.awaitTermination()

# COMMAND ----------

# DBTITLE 0,--i18n-a0ccbda9-14ff-4209-97a2-725dcee960cd
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
