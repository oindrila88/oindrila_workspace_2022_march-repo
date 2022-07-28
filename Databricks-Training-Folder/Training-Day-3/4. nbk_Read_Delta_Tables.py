# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 3
# MAGIC * Topic: How to Read "Delta Tables"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Delta Tables
# MAGIC * It is possible to "Read" a "Delta Table" as a "DataFrame" by specifying a "Table Name" or a "Path".
# MAGIC * The "DataFrame" returned automatically "Reads" the "Most Recent Snapshot" of the "Delta Table" for any "Query". There is no need to run "REFRESH TABLE".
# MAGIC * "Delta Lake" automatically uses "Partitioning" and "Statistics" to "Read" the "Minimum Amount" of "Data" when there are "Predicates" applicable in the "Query".

# COMMAND ----------

# DBTITLE 1,Read a Delta Table, Defined in the "Metastore", Using Spark SQL
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_DeltaCustomerFromDataFrame

# COMMAND ----------

# DBTITLE 1,Read a Delta Table, Defined by "Path", Using Spark SQL
# MAGIC %sql
# MAGIC SELECT * FROM delta.`/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`

# COMMAND ----------

# DBTITLE 1,Read a Delta Table, Defined in the "Metastore", Using Python
# "Query" a "Delta Table" in the "Hive Metastore".
display(spark.table("retailer_db.tbl_DeltaCustomerFromDataFrame"))

# COMMAND ----------

# DBTITLE 1,Read a Delta Table, Defined by "Path", Using Python
# "Query" a "Delta Table" using the "Path".
display(spark.read.format("delta").load("/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath"))
