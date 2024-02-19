# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-68be35ae-f2b3-48b3-b07b-de55f9ff8e80
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Ingesting Data Lab
# MAGIC
# MAGIC Read in CSV files containing products data.
# MAGIC
# MAGIC ##### Tasks
# MAGIC 1. Read with infer schema
# MAGIC 2. Read with user-defined schema
# MAGIC 3. Read with schema as DDL formatted string
# MAGIC 4. Write using Delta format

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# DBTITLE 0,--i18n-47817139-7b7a-456a-8218-2b1e08939f25
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Read with infer schema
# MAGIC - View the first CSV file using DBUtils method **`fs.head`** with the filepath provided in the variable **`single_product_cs_file_path`**
# MAGIC - Create **`products_df`** by reading from CSV files located in the filepath provided in the variable **`products_csv_path`**
# MAGIC   - Configure options to use first line as header and infer schema

# COMMAND ----------

# ANSWER
single_product_csv_file_path = f"{DA.paths.datasets}/products/products.csv/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
print(dbutils.fs.head(single_product_csv_file_path))

products_csv_path = f"{DA.paths.datasets}/products/products.csv"
products_df = (spark
               .read
               .option("header", True)
               .option("inferSchema", True)
               .csv(products_csv_path)
              )

products_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-4e08ac30-1f0e-4993-988a-2ed5024a3aa1
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

assert(products_df.count() == 12)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-10dd820e-d462-4e1f-a0cd-4010a7e38031
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Read with user-defined schema
# MAGIC Define schema by creating a **`StructType`** with column names and data types

# COMMAND ----------

# ANSWER
from pyspark.sql.types import DoubleType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True)
])

products_df2 = (spark
                .read
                .option("header", True)
                .schema(user_defined_schema)
                .csv(products_csv_path)
               )

# COMMAND ----------

# DBTITLE 0,--i18n-2286f55e-fbdc-409c-a9cb-ea224c9e6134
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

assert(user_defined_schema.fieldNames() == ["item_id", "name", "price"])
print("All test pass")

# COMMAND ----------

from pyspark.sql import Row

expected1 = Row(item_id="M_STAN_Q", name="Standard Queen Mattress", price=1045.0)
result1 = products_df2.first()

assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-1a03df94-e3ca-47c7-900f-80b300d8e015
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 3. Read with DDL formatted string

# COMMAND ----------

# ANSWER
ddl_schema = "`item_id` STRING,`name` STRING,`price` DOUBLE"

products_df3 = (spark
                .read
                .option("header", True)
                .schema(ddl_schema)
                .csv(products_csv_path)
               )

# COMMAND ----------

# DBTITLE 0,--i18n-7b00eee9-66c4-4e05-a531-a978df61a8a4
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**

# COMMAND ----------

assert(products_df3.count() == 12)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-74b30474-083a-4001-bd1f-bd6e3aa1c2b6
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 4. Write to Delta
# MAGIC Write **`products_df`** to the filepath provided in the variable **`products_output_path`**

# COMMAND ----------

# ANSWER
products_output_path = f"{DA.paths.working_dir}/delta/products"
(products_df
 .write
 .format("delta")
 .mode("overwrite")
 .save(products_output_path)
)

# COMMAND ----------

# DBTITLE 0,--i18n-9ea56c39-7402-46ec-9c46-c197ddcc1082
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

verify_files = dbutils.fs.ls(products_output_path)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == "_delta_log/":
        verify_delta_format = True
    elif f.name.endswith(".parquet"):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files > 0, "No data written"
del verify_files, verify_delta_format, verify_num_data_files
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-1a76214d-7eff-4297-85cf-0ebc7fb02605
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
