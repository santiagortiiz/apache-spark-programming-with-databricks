# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-34de9fea-de6f-464d-8132-d904ba976f5d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Sort Day Lab
# MAGIC
# MAGIC ##### Tasks
# MAGIC 1. Define a UDF to label the day of week
# MAGIC 1. Apply the UDF to label and sort by day of week
# MAGIC 1. Plot active users by day of week as a bar graph

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# DBTITLE 0,--i18n-fcf6dfd3-d3d4-409d-9f81-7529dcfeed13
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Start with a DataFrame of the average number of active users by day of week.
# MAGIC
# MAGIC This was the resulting **`df`** in a previous lab.

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct, avg, col, date_format, to_date

df = (spark
      .read
      .format("delta")
      .load(DA.paths.events)
      .withColumn("ts", (col("event_timestamp") / 1e6).cast("timestamp"))
      .withColumn("date", to_date("ts"))
      .groupBy("date").agg(approx_count_distinct("user_id").alias("active_users"))
      .withColumn("day", date_format(col("date"), "E"))
      .groupBy("day").agg(avg(col("active_users")).alias("avg_users"))
     )

display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-6e37fd9e-90db-41e4-9835-bfdaf51a323b
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Define UDF to label day of week
# MAGIC
# MAGIC Use the **`label_day_of_week`** function provided below to create the UDF **`label_dow_udf`**

# COMMAND ----------

def label_day_of_week(day: str) -> str:
    dow = {"Mon": "1", "Tue": "2", "Wed": "3", "Thu": "4",
           "Fri": "5", "Sat": "6", "Sun": "7"}
    return dow.get(day) + "-" + day

# COMMAND ----------

# TODO
label_dow_udf = FILL_IN

# COMMAND ----------

# DBTITLE 0,--i18n-b824f84c-c87a-422f-9a8f-bd217c416936
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Apply UDF to label and sort by day of week
# MAGIC - Update the **`day`** column by applying the UDF and replacing this column
# MAGIC - Sort by **`day`**
# MAGIC - Plot as a bar graph

# COMMAND ----------

# TODO
final_df = FILL_IN

display(final_df)

# COMMAND ----------

# DBTITLE 0,--i18n-cec8ed06-c40d-46d8-86c5-ecaebf65fe68
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
