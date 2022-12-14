# Databricks notebook source
# MAGIC %md
# MAGIC # Create Views in Databricks
# MAGIC * Topic: How to "Create" and "Display" the Records of "Views" Using Spark SQL in Databricks.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerAddress = spark.read\
                              .option("header", "true")\
                              .option("sep", "|")\
                              .option("inferSchema", "true")\
                              .csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")
display(df_ReadCustomerAddress)

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "View"?
# MAGIC * A "View" is a "Virtual Table" that has no physical data based on the result-set of a SQL query on which the "View" is created.

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Temporary View"?
# MAGIC * "TEMPORARY Views" are "Session-Scoped" and is "Dropped" when the "Session" ends because it "Skips Persisting the Definition" in the underlying "Metastore", if any.
# MAGIC * "TEMPORARY Views" are "Scoped" to the "Notebook" or "Script" level. Hence, "TEMPORARY Views" cannot be referenced "Outside of the Notebook" in which those are "Declared", and will no longer exist when the "Notebook" detaches from the "Cluster".

# COMMAND ----------

# MAGIC %md
# MAGIC # How to Create a "Temporary View"?
# MAGIC * 1. <b>createTempView</b>: The "<b>createTempView()</b>" Method is the "Simplest Way" to "Create" a "<b>Temporary View</b>" that "Later" can be "Used" to "Query" the "Data".
# MAGIC <br>The "<b>createTempView()</b>" Method "Takes" Only the "<b>Name</b>" of the "<b>View</b>" to be "Created" as the "<b>Parameter</b>".
# MAGIC <br>If the "<b>View Name</b> "<b>Already Exists</b>" in the "Current Session", the "<b>createTempView()</b>" Method Throws the "<b>TempTableAlreadyExistsException</b>" Exception.
# MAGIC <br><br>
# MAGIC * 2. <b>createOrReplaceTempView</b>: The "<b>createOrReplaceTempView()</b>" Method "<b>Creates</b>" a "<b>New</b>" <b>Temporary View</b>", or, "<b>Replaces</b>" the "<b>Definition</b>" of an "<b>Already Existing View</b>" that "Later" can be "Used" to "Query" the "Data".
# MAGIC <br>The "<b>createOrReplaceTempView()</b>" Method also "Takes" Only the "<b>Name</b>" of the "<b>View</b>" to be "Created" as the "<b>Parameter</b>".
# MAGIC <br>If the "<b>View Name</b> "<b>Already Exists</b>" in the "Current Session", the "<b>createOrReplaceTempView()</b>" Method just "<b>Replaces</b>" the "<b>Definition</b>" of that "<b>Already Existing View</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Temporary View" from a DataFrame Using "createTempView ()" Method of DataFrame
df_ReadCustomerAddress.createTempView("v_temp_customerAddress")

# COMMAND ----------

# DBTITLE 1,Perform a Simple SELECT Query on the Created View
display(spark.sql("SELECT * FROM v_temp_customerAddress"))

# COMMAND ----------

# DBTITLE 1,Create a "Temporary View" from a DataFrame Using "createOrReplaceTempView ()" Method of DataFrame
df_ReadCustomerAddress.select("ca_address_sk", "ca_address_id", "ca_street_number", "ca_street_name").createOrReplaceTempView("v_temp_customerAddress")

# COMMAND ----------

# DBTITLE 1,Use Magic Command "%sql" to Execute SQL Queries Directly in the Notebook
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM v_temp_customerAddress

# COMMAND ----------

# DBTITLE 1,Create a "Temporary View" from a Subset of a DataFrame
from pyspark.sql.functions import col

df_ReadCustomerAddress.\
                        select("ca_address_sk", "ca_country", "ca_state", "ca_city", "ca_street_name").\
                        where(col("ca_state").contains("AK")).\
                        createOrReplaceTempView("v_temp_AK_Addresses")

display(spark.sql("SELECT * FROM v_temp_AK_Addresses"))

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Global Temporary View"?
# MAGIC * "GLOBAL TEMPORARY Views" are also "Session-Scoped" and is "Dropped" when the "Session" ends because it "Skips Persisting the Definition" in the underlying "Metastore", if any. "GLOBAL TEMPORARY Views" are "Tied" to a System Preserved Temporary Schema "global_temp".
# MAGIC * "GLOBAL TEMPORARY Views" are "Scoped" to the "Cluster" level and can be "Shared" between "Notebooks" or "Jobs" that "Share Computing Resources".
# MAGIC * Databricks recommends using "Views" with appropriate "Table ACLs" instead of "GLOBAL TEMPORARY Views".

# COMMAND ----------

# DBTITLE 1,Create a "Global Temporary View"
df_ReadCustomerAddress.createGlobalTempView("gv_temp_customerAddress")

# COMMAND ----------

# DBTITLE 1,Perform SQL Query on a Global Temporary View
display(spark.sql("SELECT * FROM global_temp.gv_temp_customerAddress"))

# COMMAND ----------

# DBTITLE 1,Display All Available "Views" in the Current Database in Use in a Databricks Environment
display(spark.sql("show views"))

# COMMAND ----------

# DBTITLE 1,Display All Available "Global Temporary Views" in Use in a Databricks Environment
display(spark.sql("show views in global_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC # "Temporary View" and "Global Temporary View" of the "Same Name"
# MAGIC * It is possible to "Create" a "Temporary View" and a "Global Temporary View" of the "Same Name" in the "Same Spark Session".

# COMMAND ----------

# DBTITLE 1,Create a "Temporary View" by the Name "gv_temp_customerAddress"
# There is Already a "Global Temporary View", by the name "gv_temp_customerAddress".
df_ReadCustomerAddress.createTempView("gv_temp_customerAddress")

# COMMAND ----------

# DBTITLE 1,Perform a Simple SELECT Query on the Created View
display(spark.sql("SELECT * FROM gv_temp_customerAddress"))

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Materialized View"?
# MAGIC * When the "Results" of a "View Expression" are "Stored" in a "Database System", those are called "Materialized Views".
# MAGIC * "Spark SQL" also supports "Materialized Views" by "Caching" hot data in "Memory". It can be available without concerning about updates because DataFrames are "Read Only". When a user call "cache()" on a DataFrame, it will try to keep the value of this "DataFrame" in memory if possible, when this DataFrame is being materialized.
