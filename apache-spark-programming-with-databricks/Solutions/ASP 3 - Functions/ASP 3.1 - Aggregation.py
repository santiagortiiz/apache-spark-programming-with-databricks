# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-33ed5661-945e-4602-9062-1d41550d349e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Aggregation
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Group data by specified columns
# MAGIC 1. Apply grouped data methods to aggregate data
# MAGIC 1. Apply built-in functions to aggregate data
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`groupBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank" target="_blank">Grouped Data</a>: **`agg`**, **`avg`**, **`count`**, **`max`**, **`sum`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-In Functions</a>: **`approx_count_distinct`**, **`avg`**, **`sum`**

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# DBTITLE 0,--i18n-53c415db-4b5b-4572-a19b-98af284f250a
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Let's use the BedBricks events dataset.

# COMMAND ----------

df = spark.read.format("delta").load(DA.paths.events)
display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-d33fd16f-8f2c-45e0-94a1-2f5638a1f44c
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Grouping data
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/aspwd/aggregation_groupby.png" width="60%" />

# COMMAND ----------

# DBTITLE 0,--i18n-33c82540-0afb-41d5-8366-b072102ae475
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### groupBy
# MAGIC Use the DataFrame **`groupBy`** method to create a grouped data object. 
# MAGIC
# MAGIC This grouped data object is called **`RelationalGroupedDataset`** in Scala and **`GroupedData`** in Python.

# COMMAND ----------

df.groupBy("event_name")

# COMMAND ----------

df.groupBy("geo.state", "geo.city")

# COMMAND ----------

# DBTITLE 0,--i18n-de9bc0c1-8991-448d-be80-f3dd455bee06
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Grouped data methods
# MAGIC Various aggregation methods are available on the <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank">GroupedData</a> object.
# MAGIC
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | agg | Compute aggregates by specifying a series of aggregate columns |
# MAGIC | avg | Compute the mean value for each numeric columns for each group |
# MAGIC | count | Count the number of rows for each group |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | min | Compute the min value for each numeric column for each group |
# MAGIC | pivot | Pivots a column of the current DataFrame and performs the specified aggregation |
# MAGIC | sum | Compute the sum for each numeric columns for each group |

# COMMAND ----------

event_counts_df = df.groupBy("event_name").count()
display(event_counts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-1bc3f8ca-7dec-4b6a-9647-00791b82b2ac
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Here, we're getting the average purchase revenue for each.

# COMMAND ----------

avg_state_purchases_df = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
display(avg_state_purchases_df)

# COMMAND ----------

# DBTITLE 0,--i18n-a3c86855-8cf6-4832-bad1-67c2e19ae93a
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC And here the total quantity and sum of the purchase revenue for each combination of state and city.

# COMMAND ----------

city_purchase_quantities_df = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity", "ecommerce.purchase_revenue_in_usd")
display(city_purchase_quantities_df)

# COMMAND ----------

# DBTITLE 0,--i18n-41705fda-f6ec-4008-8561-3d89b337ba35
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Built-In Functions
# MAGIC In addition to DataFrame and Column transformation methods, there are a ton of helpful functions in Spark's built-in <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html" target="_blank">SQL functions</a> module.
# MAGIC
# MAGIC In Scala, this is <a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">**`org.apache.spark.sql.functions`**</a>, and <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions" target="_blank">**`pyspark.sql.functions`**</a> in Python. Functions from this module must be imported into your code.

# COMMAND ----------

# DBTITLE 0,--i18n-4c587ad6-517c-4416-8ed9-24df20602f8e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Aggregate Functions
# MAGIC
# MAGIC Here are some of the built-in functions available for aggregation.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | approx_count_distinct | Returns the approximate number of distinct items in a group |
# MAGIC | avg | Returns the average of the values in a group |
# MAGIC | collect_list | Returns a list of objects with duplicates |
# MAGIC | corr | Returns the Pearson Correlation Coefficient for two columns |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | stddev_samp | Returns the sample standard deviation of the expression in a group |
# MAGIC | sumDistinct | Returns the sum of distinct values in the expression |
# MAGIC | var_pop | Returns the population variance of the values in a group |
# MAGIC
# MAGIC Use the grouped data method <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg" target="_blank">**`agg`**</a> to apply built-in aggregate functions
# MAGIC
# MAGIC This allows you to apply other transformations on the resulting columns, such as <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html" target="_blank">**`alias`**</a>.

# COMMAND ----------

from pyspark.sql.functions import sum

state_purchases_df = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
display(state_purchases_df)

# COMMAND ----------

# DBTITLE 0,--i18n-0cbb1c99-23da-45cd-85f2-b4de3e6d2587
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Apply multiple aggregate functions on grouped data

# COMMAND ----------

from pyspark.sql.functions import avg, approx_count_distinct

state_aggregates_df = (df
                       .groupBy("geo.state")
                       .agg(avg("ecommerce.total_item_quantity").alias("avg_quantity"),
                            approx_count_distinct("user_id").alias("distinct_users"))
                      )

display(state_aggregates_df)

# COMMAND ----------

# DBTITLE 0,--i18n-fbc371d8-a5c5-46e4-856c-43e84b029d72
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Math Functions
# MAGIC Here are some of the built-in functions for math operations.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | ceil | Computes the ceiling of the given column. |
# MAGIC | cos | Computes the cosine of the given value. |
# MAGIC | log | Computes the natural logarithm of the given value. |
# MAGIC | round | Returns the value of the column e rounded to 0 decimal places with HALF_UP round mode. |
# MAGIC | sqrt | Computes the square root of the specified float value. |

# COMMAND ----------

from pyspark.sql.functions import cos, sqrt

display(spark.range(10)  # Create a DataFrame with a single column called "id" with a range of integer values
        .withColumn("sqrt", sqrt("id"))
        .withColumn("cos", cos("id"))
       )

# COMMAND ----------

# DBTITLE 0,--i18n-cfb43a90-72e5-466e-82e6-31e1f14dfbf3
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
