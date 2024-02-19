# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-c8b680c1-52ff-46a1-95a8-017dd1faf468
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Additional Functions
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Apply built-in functions to generate data for new columns
# MAGIC 1. Apply DataFrame NA functions to handle null values
# MAGIC 1. Join DataFrames
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join" target="_blank">DataFrame Methods </a>: **`join`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>: **`fill`**, **`drop`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-In Functions</a>:
# MAGIC   - Aggregate: **`collect_set`**
# MAGIC   - Collection: **`explode`**
# MAGIC   - Non-aggregate and miscellaneous: **`col`**, **`lit`**

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sales_df = spark.read.format("delta").load(DA.paths.sales)
display(sales_df)

# COMMAND ----------

# DBTITLE 0,--i18n-0fc5aedb-0dcd-4ce5-affb-742c6f476b4d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Non-aggregate and Miscellaneous Functions
# MAGIC Here are a few additional non-aggregate and miscellaneous built-in functions.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | col / column | Returns a Column based on the given column name. |
# MAGIC | lit | Creates a Column of literal value |
# MAGIC | isnull | Return true iff the column is null |
# MAGIC | rand | Generate a random column with independent and identically distributed (i.i.d.) samples uniformly distributed in [0.0, 1.0) |

# COMMAND ----------

# DBTITLE 0,--i18n-36663576-e486-4760-9b82-988bedeea35a
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC We could select a particular column using the **`col`** function

# COMMAND ----------

gmail_accounts = sales_df.filter(col("email").endswith("gmail.com"))

display(gmail_accounts)

# COMMAND ----------

# DBTITLE 0,--i18n-a503922e-7844-4001-8748-28d229a9a559
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **`lit`** can be used to create a column out of a value, which is useful for appending columns.

# COMMAND ----------

display(gmail_accounts.select("email", lit(True).alias("gmail user")))

# COMMAND ----------

# DBTITLE 0,--i18n-2b3829c9-d241-46df-9d6d-823fa84ba889
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### DataFrameNaFunctions
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a> is a DataFrame submodule with methods for handling null values. Obtain an instance of DataFrameNaFunctions by accessing the **`na`** attribute of a DataFrame.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | drop | Returns a new DataFrame omitting rows with any, all, or a specified number of null values, considering an optional subset of columns |
# MAGIC | fill | Replace null values with the specified value for an optional subset of columns |
# MAGIC | replace | Returns a new DataFrame replacing a value with another value, considering an optional subset of columns |

# COMMAND ----------

# DBTITLE 0,--i18n-e4a09833-ad6b-4e7b-b782-e832fd8752a8
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Here we'll see the row count before and after dropping rows with null/NA values.

# COMMAND ----------

print(sales_df.count())
print(sales_df.na.drop().count())

# COMMAND ----------

# DBTITLE 0,--i18n-76375049-bcf6-40f3-95e6-cdf8721a2b5e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Since the row counts are the same, we have the no null columns.  We'll need to explode items to find some nulls in columns such as items.coupon.

# COMMAND ----------

sales_exploded_df = sales_df.withColumn("items", explode(col("items")))
display(sales_exploded_df.select("items.coupon"))
print(sales_exploded_df.select("items.coupon").count())
print(sales_exploded_df.select("items.coupon").na.drop().count())

# COMMAND ----------

# DBTITLE 0,--i18n-e97ff3e5-0c2f-40c4-b73c-a6b558264191
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC We can fill in the missing coupon codes with **`na.fill`**

# COMMAND ----------

display(sales_exploded_df.select("items.coupon").na.fill("NO COUPON"))

# COMMAND ----------

# DBTITLE 0,--i18n-a61ddd73-4269-47f4-90ec-65035cdd2261
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Joining DataFrames
# MAGIC The DataFrame <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.join.html?highlight=join#pyspark.sql.DataFrame.join" target="_blank">**`join`**</a> method joins two DataFrames based on a given join expression. 
# MAGIC
# MAGIC Several different types of joins are supported:
# MAGIC
# MAGIC Inner join based on equal values of a shared column called "name" (i.e., an equi join)<br/>
# MAGIC **`df1.join(df2, "name")`**
# MAGIC
# MAGIC Inner join based on equal values of the shared columns called "name" and "age"<br/>
# MAGIC **`df1.join(df2, ["name", "age"])`**
# MAGIC
# MAGIC Full outer join based on equal values of a shared column called "name"<br/>
# MAGIC **`df1.join(df2, "name", "outer")`**
# MAGIC
# MAGIC Left outer join based on an explicit column expression<br/>
# MAGIC **`df1.join(df2, df1["customer_name"] == df2["account_name"], "left_outer")`**

# COMMAND ----------

# DBTITLE 0,--i18n-9e8fb941-044c-4640-8fad-9278a8c26a84
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC We'll load in our users data to join with our gmail_accounts from above.

# COMMAND ----------

users_df = spark.read.format("delta").load(DA.paths.users)
display(users_df)

# COMMAND ----------

joined_df = gmail_accounts.join(other=users_df, on='email', how = "inner")
display(joined_df)

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
