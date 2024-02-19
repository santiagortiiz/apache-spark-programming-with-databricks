# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-5505ea67-f96e-40fd-9c2a-bd4c644c928d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC # DataFrame & Column
# MAGIC ##### Objectives
# MAGIC 1. Construct columns
# MAGIC 1. Subset columns
# MAGIC 1. Add or replace columns
# MAGIC 1. Subset rows
# MAGIC 1. Sort rows
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`select`**, **`selectExpr`**, **`drop`**, **`withColumn`**, **`withColumnRenamed`**, **`filter`**, **`distinct`**, **`limit`**, **`sort`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`alias`**, **`isin`**, **`cast`**, **`isNotNull`**, **`desc`**, operators

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# DBTITLE 0,--i18n-b75891cc-f1e5-438b-be68-49f96665fee9
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Let's use the BedBricks events dataset.

# COMMAND ----------

events_df = spark.read.format("delta").load(DA.paths.events)
display(events_df)

# COMMAND ----------

# DBTITLE 0,--i18n-5a4814ee-e5b1-42af-8dfd-beab9b1b3ce7
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Column Expressions
# MAGIC
# MAGIC A <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a> is a logical construction that will be computed based on the data in a DataFrame using an expression
# MAGIC
# MAGIC Construct a new Column based on existing columns in a DataFrame

# COMMAND ----------

from pyspark.sql.functions import col

print(events_df.device)
print(events_df["device"])
print(col("device"))

# COMMAND ----------

# DBTITLE 0,--i18n-d1d6ab85-093e-4f04-8610-2247222e563d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Scala supports an additional syntax for creating a new Column based on existing columns in a DataFrame

# COMMAND ----------

# MAGIC %scala
# MAGIC $"device"

# COMMAND ----------

# DBTITLE 0,--i18n-cc9be674-735b-44f0-9662-dc68b370af7a
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Column Operators and Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | \*, + , <, >= | Math and comparison operators |
# MAGIC | ==, != | Equality and inequality tests (Scala operators are **`===`** and **`=!=`**) |
# MAGIC | alias | Gives the column an alias |
# MAGIC | cast, astype | Casts the column to a different data type |
# MAGIC | isNull, isNotNull, isNan | Is null, is not null, is NaN |
# MAGIC | asc, desc | Returns a sort expression based on ascending/descending order of the column |

# COMMAND ----------

# DBTITLE 0,--i18n-3ecd52b1-c84e-4d14-9e1c-c03a48ca0f6f
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Create complex expressions with existing columns, operators, and methods.

# COMMAND ----------

col("ecommerce.purchase_revenue_in_usd") + col("ecommerce.total_item_quantity")
col("event_timestamp").desc()
(col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")

# COMMAND ----------

# DBTITLE 0,--i18n-8ba468bb-ee75-4540-a35a-331e94594e30
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Here's an example of using these column expressions in the context of a DataFrame

# COMMAND ----------

rev_df = (events_df
         .filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
         .withColumn("purchase_revenue", (col("ecommerce.purchase_revenue_in_usd") * 100).cast("int"))
         .withColumn("avg_purchase_revenue", col("ecommerce.purchase_revenue_in_usd") / col("ecommerce.total_item_quantity"))
         .sort(col("avg_purchase_revenue").desc())
        )

display(rev_df)

# COMMAND ----------

# DBTITLE 0,--i18n-0100a578-427a-4956-b2d6-d98e3a53cc6a
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## DataFrame Transformation Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`limit`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |

# COMMAND ----------

# DBTITLE 0,--i18n-c89bbba2-4f55-4f22-addc-de25b23ee313
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Subset columns
# MAGIC Use DataFrame transformations to subset columns

# COMMAND ----------

# DBTITLE 0,--i18n-d57643b9-245e-450f-82b0-b77013f7b684
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### **`select()`**
# MAGIC Selects a list of columns or column based expressions

# COMMAND ----------

devices_df = events_df.select("user_id", "device")
display(devices_df)

# COMMAND ----------

from pyspark.sql.functions import col

locations_df = events_df.select(
    "user_id", 
    col("geo.city").alias("city"), 
    col("geo.state").alias("state")
)
display(locations_df)

# COMMAND ----------

# DBTITLE 0,--i18n-2ff20919-fa16-4c29-b07e-d43010c8821d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### **`selectExpr()`**
# MAGIC Selects a list of SQL expressions

# COMMAND ----------

apple_df = events_df.selectExpr("user_id", "device in ('macOS', 'iOS') as apple_user")
display(apple_df)

# COMMAND ----------

# DBTITLE 0,--i18n-eb07b09b-8dd2-4818-9f6f-7f7ad3a6b65e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### **`drop()`**
# MAGIC Returns a new DataFrame after dropping the given column, specified as a string or Column object
# MAGIC
# MAGIC Use strings to specify multiple columns

# COMMAND ----------

anonymous_df = events_df.drop("user_id", "geo", "device")
display(anonymous_df)

# COMMAND ----------

no_sales_df = events_df.drop(col("ecommerce"))
display(no_sales_df)


# COMMAND ----------

# DBTITLE 0,--i18n-a0632c28-d67f-4288-bf4e-1ade3df2b878
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Add or replace columns
# MAGIC Use DataFrame transformations to add or replace columns

# COMMAND ----------

# DBTITLE 0,--i18n-30f0fff0-fb26-4e29-bb71-6d6d21535c18
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### **`withColumn()`**
# MAGIC Returns a new DataFrame by adding a column or replacing an existing column that has the same name.

# COMMAND ----------

mobile_df = events_df.withColumn("mobile", col("device").isin("iOS", "Android"))
display(mobile_df)

# COMMAND ----------

purchase_quantity_df = events_df.withColumn("purchase_quantity", col("ecommerce.total_item_quantity").cast("int"))
purchase_quantity_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-506e9b1b-a1f9-4bc9-86f7-db3b7d48ce7a
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### **`withColumnRenamed()`**
# MAGIC Returns a new DataFrame with a column renamed.

# COMMAND ----------

location_df = events_df.withColumnRenamed("geo", "location")
display(location_df)

# COMMAND ----------

# DBTITLE 0,--i18n-ec6222b9-3810-4f96-b17d-5952f218d59c
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Subset Rows
# MAGIC Use DataFrame transformations to subset rows

# COMMAND ----------

# DBTITLE 0,--i18n-aa67adfe-8712-40e9-9c2b-840dfda38ef9
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### **`filter()`**
# MAGIC Filters rows using the given SQL expression or column based condition.
# MAGIC
# MAGIC ##### Alias: **`where`**

# COMMAND ----------

purchases_df = events_df.filter("ecommerce.total_item_quantity > 0")
display(purchases_df)

# COMMAND ----------

revenue_df = events_df.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
display(revenue_df)

# COMMAND ----------

android_df = events_df.filter((col("traffic_source") != "direct") & (col("device") == "Android"))
display(android_df)

# COMMAND ----------

# DBTITLE 0,--i18n-1699cd7f-4bd1-431d-b277-6d6004848c56
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### **`dropDuplicates()`**
# MAGIC Returns a new DataFrame with duplicate rows removed, optionally considering only a subset of columns.
# MAGIC
# MAGIC ##### Alias: **`distinct`**

# COMMAND ----------

display(events_df.distinct())

# COMMAND ----------

distinct_users_df = events_df.dropDuplicates(["user_id"])
display(distinct_users_df)

# COMMAND ----------

# DBTITLE 0,--i18n-e8daf914-afef-4159-9414-23bde2efd76a
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### **`limit()`**
# MAGIC Returns a new DataFrame by taking the first n rows.

# COMMAND ----------

limit_df = events_df.limit(100)
display(limit_df)

# COMMAND ----------

# DBTITLE 0,--i18n-06c7f4a5-02b7-44f6-a29d-213ee2976d48
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Sort rows
# MAGIC Use DataFrame transformations to sort rows

# COMMAND ----------

# DBTITLE 0,--i18n-50c39ed4-f70d-45b7-a452-22b819c0bb30
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### **`sort()`**
# MAGIC Returns a new DataFrame sorted by the given columns or expressions.
# MAGIC
# MAGIC ##### Alias: **`orderBy`**

# COMMAND ----------

increase_timestamps_df = events_df.sort("event_timestamp")
display(increase_timestamps_df)

# COMMAND ----------

decrease_timestamp_df = events_df.sort(col("event_timestamp").desc())
display(decrease_timestamp_df)

# COMMAND ----------

increase_sessions_df = events_df.orderBy(["user_first_touch_timestamp", "event_timestamp"])
display(increase_sessions_df)

# COMMAND ----------

decrease_sessions_df = events_df.sort(col("user_first_touch_timestamp").desc(), col("event_timestamp"))
display(decrease_sessions_df)

# COMMAND ----------

# DBTITLE 0,--i18n-0062c495-c719-453d-a3eb-eb495d5bafd3
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
