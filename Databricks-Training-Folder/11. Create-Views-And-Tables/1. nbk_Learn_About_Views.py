# Databricks notebook source
# MAGIC %md
# MAGIC # Create Views in Databricks
# MAGIC * Topic: How to "Create" and "Display" the Records of "Views" Using Spark SQL in Databricks.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerAddress = spark.read\
                              .csv(path = "dbfs:/FileStore/tables/retailer/data/customer_address.dat", sep = "|", header = True, inferSchema = True)
display(df_ReadCustomerAddress)

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "View"?
# MAGIC * A "<b>View</b>" is a "<b>Virtual Table</b>" that has "<b>No Physical Data</b>".
# MAGIC * A "<b>View</b>" is "<b>Created</b>" based on the "<b>Result-Set</b>" of a "<b>SQL Query</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Temporary View"?
# MAGIC * A "<b>Temporary View</b>" is "<b>Session-Scoped</b>" and is "<b>Dropped</b>" when the "<b>Session Ends</b>", because the "<b>Definition</b>" of the "<b>Temporary View</b>" is "<b>Not Stored</b>" in the underlying "<b>Metastore</b>", if any.
# MAGIC * A "<b>Temporary View</b>" is "<b>Scoped</b>" to the "<b>Notebook</b>" level. Hence, a A "<b>Temporary View</b>" can "<b>Not be Referenced Outside of the Notebook in which it is Declared</b>", and will "<b>No Longer Exist</b>" when the "<b>Notebook Detaches from the Cluster</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How to Create a "Temporary View"?
# MAGIC * 1. <b>createTempView</b>: The "<b>createTempView()</b>" Method is the "Simplest Way" to "Create" a "<b>Temporary View</b>" that "Later" can be "Used" to "Query" the "Data".
# MAGIC <br>The "<b>createTempView()</b>" Method "Takes" Only the "<b>Name</b>" of the "<b>View</b>" to be "Created" as the "<b>Parameter</b>".
# MAGIC <br>If the "<b>View Name</b>" "<b>Already Exists</b>" in the "<b>Current Session</b>", the "<b>createTempView()</b>" Method Throws the "<b>TempTableAlreadyExistsException</b>" Exception.
# MAGIC <br><br>
# MAGIC * 2. <b>createOrReplaceTempView</b>: The "<b>createOrReplaceTempView()</b>" Method "<b>Creates</b>" a "<b>New</b>" <b>Temporary View</b>", or, "<b>Replaces</b>" the "<b>Definition</b>" of an "<b>Already Existing View</b>" that "Later" can be "Used" to "Query" the "Data".
# MAGIC <br>The "<b>createOrReplaceTempView()</b>" Method also "Takes" Only the "<b>Name</b>" of the "<b>View</b>" to be "Created" as the "<b>Parameter</b>".
# MAGIC <br>If the "<b>View Name</b>" "<b>Already Exists</b>" in the "<b>Current Session</b>", the "<b>createOrReplaceTempView()</b>" Method just "<b>Replaces</b>" the "<b>Definition</b>" of that "<b>Already Existing View</b>".

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
# MAGIC * A "<b>Global Temporary View</b>" is also "<b>Session-Scoped</b>" and is "<b>Dropped</b>" when the "<b>Session Ends</b>", because the "<b>Definition</b>" of the "<b>Global Temporary View</b>" is "<b>Not Stored</b>" in the underlying "<b>Metastore</b>", if any.
# MAGIC * "<b>Global Temporary Views</b>" are "<b>Tied</b>" to a "<b>System Preserved Temporary Schema</b>", called "<b>global_temp</b>".
# MAGIC * A "<b>Global Temporary View</b>" is "<b>Scoped</b>" to the "<b>Cluster</b>" level, and, can be "<b>Shared</b>" between the "<b>Notebooks</b>" or "<b>Jobs</b>" that "<b>Share</b>" the "<b>Computing Resources</b>" of that "<b>Cluster</b>".
# MAGIC * "<b>Databricks</b>" recommends using "<b>Views</b>" with "<b>Appropriate Table ACLs</b>", instead of "<b>Global Temporary Views</b>".

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
# MAGIC * It is possible to "<b>Create</b>" a "<b>Temporary View</b>" and a "<b>Global Temporary View</b>" of the "<b>Same Name</b>" in the "<b>Same Spark Session</b>", because, the "<b>Global Temporary View</b>" will be "<b>Stored</b>" in the "<b>System Preserved Temporary Schema</b>", called "<b>global_temp</b>", whereas, the "<b>Temporary View</b>" will be "<b>Stored</b>" in either the "<b>Database</b>" named "<b>default</b>", or, in the "<b>Database</b>", which will be provided with the "<b>Name</b>" of the "<b>Temporary View</b>" as the "<b>Schema</b>" of the "<b>Temporary View</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Temporary View" by the Name "gv_temp_customerAddress"
# There is Already a "Global Temporary View", by the name "gv_temp_customerAddress".
df_ReadCustomerAddress.createTempView("gv_temp_customerAddress")

# COMMAND ----------

# DBTITLE 1,Perform a Simple SELECT Query on the Created View
display(spark.sql("SELECT * FROM gv_temp_customerAddress"))
