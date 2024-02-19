# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-b6890eab-5e31-49be-88c7-792f32f49a23
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Databricks Platform
# MAGIC
# MAGIC Demonstrate basic functionality and identify terms related to working in the Databricks workspace.
# MAGIC
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Execute code in multiple languages
# MAGIC 1. Create documentation cells
# MAGIC 1. Access DBFS (Databricks File System)
# MAGIC 1. Create database and table
# MAGIC 1. Query table and plot results
# MAGIC 1. Add notebook parameters with widgets
# MAGIC
# MAGIC
# MAGIC ##### Databricks Notebook Utilities
# MAGIC - <a href="https://docs.databricks.com/notebooks/notebooks-use.html#language-magic" target="_blank">Magic commands</a>: **`%python`**, **`%scala`**, **`%sql`**, **`%r`**, **`%sh`**, **`%md`**
# MAGIC - <a href="https://docs.databricks.com/dev-tools/databricks-utils.html" target="_blank">DBUtils</a>: **`dbutils.fs`** (**`%fs`**), **`dbutils.notebooks`** (**`%run`**), **`dbutils.widgets`**
# MAGIC - <a href="https://docs.databricks.com/notebooks/visualizations/index.html" target="_blank">Visualization</a>: **`display`**, **`displayHTML`**

# COMMAND ----------

# DBTITLE 0,--i18n-763aac82-c507-44e0-b2a7-f5ebd42cb64c
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Setup
# MAGIC Run classroom setup to <a href="https://docs.databricks.com/data/databricks-file-system.html#mount-storage" target="_blank">mount</a> Databricks training datasets and create your own database for BedBricks.
# MAGIC
# MAGIC Use the **`%run`** magic command to run another notebook within a notebook

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# DBTITLE 0,--i18n-d4b5cfdc-1842-4c68-983a-600956e46644
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Execute code in multiple languages
# MAGIC Run default language of notebook

# COMMAND ----------

print("Run default language")

# COMMAND ----------

# DBTITLE 0,--i18n-866eea11-4467-4192-8263-79510da3966d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Run language specified by language magic commands: **`%python`**, **`%scala`**, **`%sql`**, **`%r`**

# COMMAND ----------

print("Run python")

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Run scala")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Run SQL"

# COMMAND ----------

# MAGIC %r
# MAGIC print("Run R", quote=FALSE)

# COMMAND ----------

# DBTITLE 0,--i18n-905f4010-a884-4d3d-a84e-6cdc5b3ea493
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Run shell commands on the driver using the magic command: **`%sh`**

# COMMAND ----------

# MAGIC %sh ps | grep 'java'

# COMMAND ----------

# DBTITLE 0,--i18n-0a5ea494-a63c-4ea8-b0d9-d374dc1ebb2f
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Render HTML using the function: **`displayHTML`** (available in Python, Scala, and R)

# COMMAND ----------

html = """<h1 style="color:orange;text-align:center;font-family:Courier">Render HTML</h1>"""
displayHTML(html)

# COMMAND ----------

# DBTITLE 0,--i18n-03da2bfd-0e21-4650-9a80-97bbfd6b5b4f
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Create documentation cells
# MAGIC Render cell as <a href="https://www.markdownguide.org/cheat-sheet/" target="_blank">Markdown</a> using the magic command: **`%md`**
# MAGIC
# MAGIC Below are some examples of how you can use Markdown to format documentation. Click this cell and press **`Enter`** to view the underlying Markdown syntax.
# MAGIC
# MAGIC
# MAGIC # Heading 1
# MAGIC ### Heading 3
# MAGIC > block quote
# MAGIC
# MAGIC 1. **bold**
# MAGIC 2. *italicized*
# MAGIC 3. ~~strikethrough~~
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC - <a href="https://www.markdownguide.org/cheat-sheet/" target="_blank">link</a>
# MAGIC - `code`
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "message": "This is a code block",
# MAGIC   "method": "https://www.markdownguide.org/extended-syntax/#fenced-code-blocks",
# MAGIC   "alternative": "https://www.markdownguide.org/basic-syntax/#code-blocks"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ![Spark Logo](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC
# MAGIC | Element         | Markdown Syntax |
# MAGIC |-----------------|-----------------|
# MAGIC | Heading         | `#H1` `##H2` `###H3` `#### H4` `##### H5` `###### H6` |
# MAGIC | Block quote     | `> blockquote` |
# MAGIC | Bold            | `**bold**` |
# MAGIC | Italic          | `*italicized*` |
# MAGIC | Strikethrough   | `~~strikethrough~~` |
# MAGIC | Horizontal Rule | `---` |
# MAGIC | Code            | ``` `code` ``` |
# MAGIC | Link            | `[text](https://www.example.com)` |
# MAGIC | Image           | `![alt text](image.jpg)`|
# MAGIC | Ordered List    | `1. First items` <br> `2. Second Item` <br> `3. Third Item` |
# MAGIC | Unordered List  | `- First items` <br> `- Second Item` <br> `- Third Item` |
# MAGIC | Code Block      | ```` ``` ```` <br> `code block` <br> ```` ``` ````|
# MAGIC | Table           |<code> &#124; col &#124; col &#124; col &#124; </code> <br> <code> &#124;---&#124;---&#124;---&#124; </code> <br> <code> &#124; val &#124; val &#124; val &#124; </code> <br> <code> &#124; val &#124; val &#124; val &#124; </code> <br>|

# COMMAND ----------

# DBTITLE 0,--i18n-55bcf039-68d2-4600-ab33-3b68e17cde20
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Access DBFS (Databricks File System)
# MAGIC The <a href="https://docs.databricks.com/data/databricks-file-system.html" target="_blank">Databricks File System</a> (DBFS) is a virtual file system that allows you to treat cloud object storage as though it were local files and directories on the cluster.
# MAGIC
# MAGIC Run file system commands on DBFS using the magic command: **`%fs`**
# MAGIC
# MAGIC <br/>
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"/>
# MAGIC Replace the instances of <strong>FILL_IN</strong> in the cells below with your email address:

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp

# COMMAND ----------

# MAGIC %fs put dbfs:/tmp/FILL_IN.txt "This is a test of the emergency broadcast system, this is only a test" --overwrite=true

# COMMAND ----------

# MAGIC %fs head dbfs:/tmp/FILL_IN.txt

# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp

# COMMAND ----------

# DBTITLE 0,--i18n-c1a4308d-ee83-4d09-8609-dbf74129c47a
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **`%fs`** is shorthand for the <a href="https://docs.databricks.com/dev-tools/databricks-utils.html" target="_blank">DBUtils</a> module: **`dbutils.fs`**

# COMMAND ----------

# MAGIC %fs help

# COMMAND ----------

# DBTITLE 0,--i18n-d096dd27-4253-4242-b882-241a251413d1
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Run file system commands on DBFS using DBUtils directly

# COMMAND ----------

dbutils.fs.ls("dbfs:/tmp")

# COMMAND ----------

# DBTITLE 0,--i18n-df97995d-6355-4002-adf0-8e27da62ad89
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Visualize results in a table using the Databricks <a href="https://docs.databricks.com/notebooks/visualizations/index.html#display-function-1" target="_blank">display</a> function

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/tmp")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-786321eb-dac1-4fb3-9a1b-e4f6be665b05
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Let's take one more look at our temp file...

# COMMAND ----------

file_name = "dbfs:/tmp/FILL_IN.txt"
contents = dbutils.fs.head(file_name)

print("-"*80)
print(contents)
print("-"*80)

# COMMAND ----------

# DBTITLE 0,--i18n-da682b0a-785c-4fbb-998c-9735849e5d66
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Our First Table
# MAGIC
# MAGIC Is located in the path identfied by **`DA.paths.events`** (a variable we created for you).
# MAGIC
# MAGIC We can see those files by running the following cell

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.events)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-fe760001-4ad5-4875-9908-0d2621b2b379
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## But, Wait!
# MAGIC I cannot use variables in SQL commands.
# MAGIC
# MAGIC With the following trick you can!
# MAGIC
# MAGIC Declare the python variable as a variable in the spark context which SQL commands can access:

# COMMAND ----------

spark.conf.set("whatever.events", DA.paths.events)

# COMMAND ----------

# DBTITLE 0,--i18n-9f524915-58be-49e0-b62e-948eac9a57f4
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> In the above example we use **`whatever.`** to give our variable a "namespace".
# MAGIC
# MAGIC This is so that we don't accidently step over other configuration parameters.
# MAGIC
# MAGIC You will see throughout this course our usage of the "DA" namesapce as in **`DA.paths.some_file`**

# COMMAND ----------

# DBTITLE 0,--i18n-cc9fe8d7-8380-4412-8e71-0ca40c8849c6
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Create table
# MAGIC Run <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/index.html#sql-reference" target="_blank">Databricks SQL Commands</a> to create a table named **`events`** using BedBricks event files on DBFS.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS events
# MAGIC USING DELTA
# MAGIC OPTIONS (path = "${whatever.events}");

# COMMAND ----------

# DBTITLE 0,--i18n-c5a68136-0804-4ca0-bffb-a42ee61674f6
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC This table was saved in the database created for you in classroom setup.
# MAGIC
# MAGIC See database name printed below.

# COMMAND ----------

print(f"Database Name: {DA.schema_name}")

# COMMAND ----------

# DBTITLE 0,--i18n-48ad8e5e-247a-494e-90ea-b794075c34d6
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ... or even the tables in that database:

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ${DA.schema_name}

# COMMAND ----------

# DBTITLE 0,--i18n-943bbcdc-07b1-4ef1-b6b3-964aee4cd396
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC View your database and table in the Data tab of the UI.

# COMMAND ----------

# DBTITLE 0,--i18n-8a01566b-882e-431e-bb33-faee48cccef0
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Query table and plot results
# MAGIC Use SQL to query the **`events`** table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM events

# COMMAND ----------

# DBTITLE 0,--i18n-0ccc0218-d091-4ab5-bb0e-bfa40bbfa541
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Run the query below and then <a href="https://docs.databricks.com/notebooks/visualizations/index.html#plot-types" target="_blank">plot</a> results by clicking the plus sign (+) and selecting *Visualization*. When presented with a bar chart, click *Save* to add it to the output window.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT traffic_source, SUM(ecommerce.purchase_revenue_in_usd) AS total_revenue
# MAGIC FROM events
# MAGIC GROUP BY traffic_source

# COMMAND ----------

# DBTITLE 0,--i18n-422a57bc-d416-4b62-813c-d2e9fe39b36f
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Add notebook parameters with widgets
# MAGIC Use <a href="https://docs.databricks.com/notebooks/widgets.html" target="_blank">widgets</a> to add input parameters to your notebook.
# MAGIC
# MAGIC Create a text input widget using SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT state DEFAULT "CA"

# COMMAND ----------

# DBTITLE 0,--i18n-a397f179-d832-47dd-8b5c-c955bd2a1a39
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Access the current value of the widget using the function **`getArgument`**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM events
# MAGIC WHERE geo.state = getArgument("state")

# COMMAND ----------

# DBTITLE 0,--i18n-463810ae-8c7e-4551-9383-27757387a49e
# MAGIC %md
# MAGIC
# MAGIC Remove the text widget

# COMMAND ----------

# MAGIC %sql
# MAGIC REMOVE WIDGET state

# COMMAND ----------

# DBTITLE 0,--i18n-4b96b453-7c54-4dec-8099-f53fff57305d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC To create widgets in Python, Scala, and R, use the DBUtils module: **`dbutils.widgets`**

# COMMAND ----------

dbutils.widgets.text("name", "Brickster", "Name")
dbutils.widgets.multiselect("colors", "orange", ["red", "orange", "black", "blue"], "Favorite Color?")

# COMMAND ----------

# DBTITLE 0,--i18n-e7c70df9-70a2-4017-8725-0f6e255184b4
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Access the current value of the widget using the **`dbutils.widgets`** function **`get`**

# COMMAND ----------

name = dbutils.widgets.get("name")
colors = dbutils.widgets.get("colors").split(",")

html = "<div>Hi {}! Select your color preference.</div>".format(name)
for c in colors:
    html += """<label for="{}" style="color:{}"><input type="radio"> {}</label><br>""".format(c, c, c)

displayHTML(html)

# COMMAND ----------

# DBTITLE 0,--i18n-c0cea1c1-bad1-4a9a-bbaf-3bc39cd7ae75
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Remove all widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 0,--i18n-dd02334a-23bf-49e5-8147-66984d8892dd
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Clean up classroom
# MAGIC Clean up any temp files, tables and databases created by this lesson

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
