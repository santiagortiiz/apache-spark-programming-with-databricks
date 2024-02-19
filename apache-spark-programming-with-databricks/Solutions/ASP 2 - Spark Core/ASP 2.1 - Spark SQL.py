# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-4e68432b-fcb4-473c-83dc-da733742ad49
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Spark SQL
# MAGIC
# MAGIC Demonstrate fundamental concepts in Spark SQL using the DataFrame API.
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Run a SQL query
# MAGIC 1. Create a DataFrame from a table
# MAGIC 1. Write the same query using DataFrame transformations
# MAGIC 1. Trigger computation with DataFrame actions
# MAGIC 1. Convert between DataFrames and SQL
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">SparkSession</a>: **`sql`**, **`table`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>:
# MAGIC   - Transformations:  **`select`**, **`where`**, **`orderBy`**
# MAGIC   - Actions: **`show`**, **`count`**, **`take`**
# MAGIC   - Other methods: **`printSchema`**, **`schema`**, **`createOrReplaceTempView`**

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-SQL

# COMMAND ----------

# DBTITLE 0,--i18n-2b97b70b-d032-48cc-a86d-4bcc3bb53c25
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Multiple Interfaces
# MAGIC Spark SQL is a module for structured data processing with multiple interfaces.
# MAGIC
# MAGIC We can interact with Spark SQL in two ways:
# MAGIC 1. Executing SQL queries
# MAGIC 1. Working with the DataFrame API.

# COMMAND ----------

# DBTITLE 0,--i18n-5daeff66-5dae-4f1e-9a1f-d99b613ae624
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC **Method 1: Executing SQL queries**
# MAGIC
# MAGIC This is how we interacted with Spark SQL in the previous lesson.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, price
# MAGIC FROM products
# MAGIC WHERE price < 200
# MAGIC ORDER BY price

# COMMAND ----------

# DBTITLE 0,--i18n-0f5a4329-12b5-4b81-875c-2722dbdf85d9
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Method 2: Working with the DataFrame API**
# MAGIC
# MAGIC We can also express Spark SQL queries using the DataFrame API.
# MAGIC The following cell returns a DataFrame containing the same results as those retrieved above.

# COMMAND ----------

display(spark
        .table("products")
        .select("name", "price")
        .where("price < 200")
        .orderBy("price")
       )

# COMMAND ----------

# DBTITLE 0,--i18n-643d5cd6-3365-4bda-8f9d-fb3107c20e16
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC We'll go over the syntax for the DataFrame API later in the lesson, but you can see this builder design pattern allows us to chain a sequence of operations very similar to those we find in SQL.

# COMMAND ----------

# DBTITLE 0,--i18n-ad108597-61bc-4dcc-9d3c-8b8a16753636
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Query Execution
# MAGIC We can express the same query using any interface. The Spark SQL engine generates the same query plan used to optimize and execute on our Spark cluster.
# MAGIC
# MAGIC ![query execution engine](https://files.training.databricks.com/images/aspwd/spark_sql_query_execution_engine.png)
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> Resilient Distributed Datasets (RDDs) are the low-level representation of datasets processed by a Spark cluster. In early versions of Spark, you had to write <a href="https://spark.apache.org/docs/latest/rdd-programming-guide.html" target="_blank">code manipulating RDDs directly</a>. In modern versions of Spark you should instead use the higher-level DataFrame APIs, which Spark automatically compiles into low-level RDD operations.

# COMMAND ----------

# DBTITLE 0,--i18n-c3b5d922-be36-40a4-b547-f574b52ca0ec
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Spark API Documentation
# MAGIC
# MAGIC To learn how we work with DataFrames in Spark SQL, let's first look at the Spark API documentation.
# MAGIC The main Spark <a href="https://spark.apache.org/docs/latest/" target="_blank">documentation</a> page includes links to API docs and helpful guides for each version of Spark.
# MAGIC
# MAGIC The <a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/index.html" target="_blank">Scala API</a> and <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">Python API</a> are most commonly used, and it's often helpful to reference the documentation for both languages.
# MAGIC Scala docs tend to be more comprehensive, and Python docs tend to have more code examples.
# MAGIC
# MAGIC #### Navigating Docs for the Spark SQL Module
# MAGIC Find the Spark SQL module by navigating to **`org.apache.spark.sql`** in the Scala API or **`pyspark.sql`** in the Python API.
# MAGIC The first class we'll explore in this module is the **`SparkSession`** class. You can find this by entering "SparkSession" in the search bar.

# COMMAND ----------

# DBTITLE 0,--i18n-e630c320-f786-4259-a656-9094cfd6c677
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## SparkSession
# MAGIC The **`SparkSession`** class is the single entry point to all functionality in Spark using the DataFrame API.
# MAGIC
# MAGIC In Databricks notebooks, the SparkSession is created for you, stored in a variable called **`spark`**.

# COMMAND ----------

spark

# COMMAND ----------

# DBTITLE 0,--i18n-d507c25b-0400-447e-a668-9b0a919ed38a
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC The example from the beginning of this lesson used the SparkSession method **`table`** to create a DataFrame from the **`products`** table. Let's save this in the variable **`products_df`**.

# COMMAND ----------

products_df = spark.table("products")

# COMMAND ----------

# DBTITLE 0,--i18n-868ac2d6-e824-4be0-a8d6-cdb2b2ee041b
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Below are several additional methods we can use to create DataFrames. All of these can be found in the <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">documentation</a> for **`SparkSession`**.
# MAGIC
# MAGIC #### **`SparkSession`** Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | sql | Returns a DataFrame representing the result of the given query |
# MAGIC | table | Returns the specified table as a DataFrame |
# MAGIC | read | Returns a DataFrameReader that can be used to read data in as a DataFrame |
# MAGIC | range | Create a DataFrame with a column containing elements in a range from start to end (exclusive) with step value and number of partitions |
# MAGIC | createDataFrame | Creates a DataFrame from a list of tuples, primarily used for testing |

# COMMAND ----------

# DBTITLE 0,--i18n-0cb0cd98-a286-45b7-8ec7-d1f2094a1571
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Let's use a SparkSession method to run SQL.

# COMMAND ----------

result_df = spark.sql("""
SELECT name, price
FROM products
WHERE price < 200
ORDER BY price
""")

display(result_df)

# COMMAND ----------

# DBTITLE 0,--i18n-bb9997aa-1f07-4fff-912c-1e4350db83c0
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## DataFrames
# MAGIC Recall that expressing our query using methods in the DataFrame API returns results in a DataFrame. Let's store this in the variable **`budget_df`**.
# MAGIC
# MAGIC A **DataFrame** is a distributed collection of data grouped into named columns.

# COMMAND ----------

budget_df = (spark
             .table("products")
             .select("name", "price")
             .where("price < 200")
             .orderBy("price")
            )

# COMMAND ----------

# DBTITLE 0,--i18n-2a074b60-9587-43b5-9d57-282b219c6b39
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC We can use **`display()`** to output the results of a dataframe.

# COMMAND ----------

display(budget_df)

# COMMAND ----------

# DBTITLE 0,--i18n-ffcd868b-ce9d-4cd8-bb53-066076a85927
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC The **schema** defines the column names and types of a dataframe.
# MAGIC
# MAGIC Access a dataframe's schema using the **`schema`** attribute.

# COMMAND ----------

budget_df.schema

# COMMAND ----------

# DBTITLE 0,--i18n-fb94b6a7-735e-4050-bedf-53e778b87beb
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC View a nicer output for this schema using the **`printSchema()`** method.

# COMMAND ----------

budget_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-414bd505-67bc-4c05-a87b-9adf23d9b50d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Transformations
# MAGIC When we created **`budget_df`**, we used a series of DataFrame transformation methods e.g. **`select`**, **`where`**, **`orderBy`**.
# MAGIC
# MAGIC <strong><code>products_df  
# MAGIC &nbsp;  .select("name", "price")  
# MAGIC &nbsp;  .where("price < 200")  
# MAGIC &nbsp;  .orderBy("price")  
# MAGIC </code></strong>
# MAGIC     
# MAGIC Transformations operate on and return DataFrames, allowing us to chain transformation methods together to construct new DataFrames.
# MAGIC However, these operations can't execute on their own, as transformation methods are **lazily evaluated**.
# MAGIC
# MAGIC Running the following cell does not trigger any computation.

# COMMAND ----------

(products_df
  .select("name", "price")
  .where("price < 200")
  .orderBy("price"))

# COMMAND ----------

# DBTITLE 0,--i18n-d8f72c04-d483-40a6-b91b-854699c12a3e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Actions
# MAGIC Conversely, DataFrame actions are methods that **trigger computation**.
# MAGIC Actions are needed to trigger the execution of any DataFrame transformations.
# MAGIC
# MAGIC The **`show`** action causes the following cell to execute transformations.

# COMMAND ----------

(products_df
  .select("name", "price")
  .where("price < 200")
  .orderBy("price")
  .show())

# COMMAND ----------

# DBTITLE 0,--i18n-43ccc1f1-5398-4715-bcbe-b071d0ac93db
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Below are several examples of <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#dataframe-apis" target="_blank">DataFrame</a> actions.
# MAGIC
# MAGIC ### DataFrame Action Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | show | Displays the top n rows of DataFrame in a tabular form |
# MAGIC | count | Returns the number of rows in the DataFrame |
# MAGIC | describe,  summary | Computes basic statistics for numeric and string columns |
# MAGIC | first, head | Returns the the first row |
# MAGIC | collect | Returns an array that contains all rows in this DataFrame |
# MAGIC | take | Returns an array of the first n rows in the DataFrame |

# COMMAND ----------

# DBTITLE 0,--i18n-a7ef182c-f9d5-41d4-a36b-327b4c3c8d45
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC **`count`** returns the number of records in a DataFrame.

# COMMAND ----------

budget_df.count()

# COMMAND ----------

# DBTITLE 0,--i18n-5c622ec1-48f8-435f-9ee8-08d9de14ec40
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC **`collect`** returns an array of all rows in a DataFrame.

# COMMAND ----------

budget_df.collect()

# COMMAND ----------

# DBTITLE 0,--i18n-477b58cf-e178-44fe-9495-849f4fed7dfe
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Convert between DataFrames and SQL

# COMMAND ----------

# DBTITLE 0,--i18n-e30d48ad-04f4-48e2-8b80-675ab907d99b
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC **`createOrReplaceTempView`** creates a temporary view based on the DataFrame. The lifetime of the temporary view is tied to the SparkSession that was used to create the DataFrame.

# COMMAND ----------

budget_df.createOrReplaceTempView("budget")

# COMMAND ----------

display(spark.sql("SELECT * FROM budget"))

# COMMAND ----------

# DBTITLE 0,--i18n-53a419c0-e7ad-4d43-aa8e-6899cbd43e8d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Classroom Cleanup

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
