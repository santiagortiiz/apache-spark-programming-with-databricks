# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-536ff4eb-272a-489d-8bb9-59f9ef0de847
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Revenue by Traffic Lab
# MAGIC Get the 3 traffic sources generating the highest total revenue.
# MAGIC 1. Aggregate revenue by traffic source
# MAGIC 2. Get top 3 traffic sources by total revenue
# MAGIC 3. Clean revenue columns to have two decimal places
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`groupBy`**, **`sort`**, **`limit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`alias`**, **`desc`**, **`cast`**, **`operators`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-in Functions</a>: **`avg`**, **`sum`**

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# DBTITLE 0,--i18n-e6e01ed4-02eb-4b48-8b90-84d440c515a0
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Setup
# MAGIC Run the cell below to create the starting DataFrame **`df`**.

# COMMAND ----------

from pyspark.sql.functions import col

# Purchase events logged on the BedBricks website
df = (spark.read.format("delta").load(DA.paths.events)
      .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
      .filter(col("revenue").isNotNull())
      .drop("event_name")
     )

display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-917ddee5-b24d-4f28-9931-b251e5708c35
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Aggregate revenue by traffic source
# MAGIC - Group by **`traffic_source`**
# MAGIC - Get sum of **`revenue`** as **`total_rev`**. 
# MAGIC - Get average of **`revenue`** as **`avg_rev`**
# MAGIC
# MAGIC Remember to import any necessary built-in functions.

# COMMAND ----------

# TODO

traffic_df = (df.FILL_IN
)

display(traffic_df)

# COMMAND ----------

# DBTITLE 0,--i18n-3c5e0ac8-5f3f-4848-8403-dae36db0d3b5
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.functions import round

expected1 = [(12704560.0, 1083.175), (78800000.3, 983.2915), (24797837.0, 1076.6221), (47218429.0, 1086.8303), (16177893.0, 1083.4378), (8044326.0, 1087.218)]
test_df = traffic_df.sort("traffic_source").select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result1 = [(row.total_rev, row.avg_rev) for row in test_df.collect()]

assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-718d5dd8-6d52-442d-88af-ecf838d5deb8
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Get top three traffic sources by total revenue
# MAGIC - Sort by **`total_rev`** in descending order
# MAGIC - Limit to first three rows

# COMMAND ----------

# TODO
top_traffic_df = (traffic_df.FILL_IN
)
display(top_traffic_df)

# COMMAND ----------

# DBTITLE 0,--i18n-a6f740e0-3808-4626-9c60-924cbcaafd5a
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

expected2 = [(78800000.3, 983.2915), (47218429.0, 1086.8303), (24797837.0, 1076.6221)]
test_df = top_traffic_df.select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result2 = [(row.total_rev, row.avg_rev) for row in test_df.collect()]

assert(expected2 == result2)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-72051bbe-4207-44b1-95ef-f5beee6d10e2
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 3. Limit revenue columns to two decimal places
# MAGIC - Modify columns **`avg_rev`** and **`total_rev`** to contain numbers with two decimal places
# MAGIC   - Use **`withColumn()`** with the same names to replace these columns
# MAGIC   - To limit to two decimal places, multiply each column by 100, cast to long, and then divide by 100

# COMMAND ----------

# TODO
final_df = (top_traffic_df.FILL_IN
)

display(final_df)

# COMMAND ----------

# DBTITLE 0,--i18n-e90f2db4-1f38-4c46-87de-2760f96b2de8
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**

# COMMAND ----------

expected3 = [(78800000.29, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
result3 = [(row.total_rev, row.avg_rev) for row in final_df.collect()]

assert(expected3 == result3)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-060c0140-3c4c-4558-96de-f7c2379742d3
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 4. Bonus: Rewrite using a built-in math function
# MAGIC Find a built-in math function that rounds to a specified number of decimal places

# COMMAND ----------

# TODO
bonus_df = (top_traffic_df.FILL_IN
)

display(bonus_df)

# COMMAND ----------

# DBTITLE 0,--i18n-9875e048-7810-4019-9891-712d6b7a5256
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

expected4 = [(78800000.3, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
result4 = [(row.total_rev, row.avg_rev) for row in bonus_df.collect()]

assert(expected4 == result4)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-085d88db-a489-4142-9e04-47687adcf61d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 5. Chain all the steps above

# COMMAND ----------

# TODO
chain_df = (df.FILL_IN
)

display(chain_df)

# COMMAND ----------

# DBTITLE 0,--i18n-7ca119b7-9322-4f04-9c58-c7873a1ea474
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **5.1: CHECK YOUR WORK**

# COMMAND ----------

method_a = [(78800000.3,  983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
method_b = [(78800000.29, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
result5 = [(row.total_rev, row.avg_rev) for row in chain_df.collect()]

assert result5 == method_a or result5 == method_b
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-60b8db05-ebb2-4d50-bed2-c72259929b16
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
