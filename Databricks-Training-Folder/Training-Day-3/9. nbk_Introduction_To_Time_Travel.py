# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 3
# MAGIC * Topic: How to Query an "Older Snapshot" of a "Delta Table" ("Time Travel")?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to "Time Travel"
# MAGIC * "Delta Lake Time Travel" allows to "Query" an "Older Snapshot" of a "Delta Table". The "Time Travel" has many "Use Cases", including -
# MAGIC * A. "Re-creating" the "Analyses", "Reports", or "Outputs" (e.g., the "Output" of a "Machine Learning Model"). This could be useful for "Debugging" or "Auditing", especially in "Regulated Industries".
# MAGIC * B. Writing the "Complex Temporal Queries".
# MAGIC * C. Fixing the "Mistakes" in the "Data".
# MAGIC * Providing the "Snapshot Isolation" for a "Set of Queries" for "Fast Changing Tables".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Time Travel" Using "TIMESTAMP AS OF"
# MAGIC * The "Timestamp Expression" can be any one of the following type -
# MAGIC * A. "String" that can be "Cast" to a "Timestamp", like - '2018-10-18T22:15:12.013Z' or cast('2018-10-18 13:36:32 CEST' as timestamp)
# MAGIC * B. "Date String", like - '2018-10-18'.
# MAGIC * C. Any "Other Expression" that is or can be "Cast" to a "Timestamp".

# COMMAND ----------

# DBTITLE 1,Query an "Older Snapshot" of a "Delta Table", Defined in the "Metastore", Using SQL "TIMESTAMP AS OF" Syntax
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_DeltaCustomerFromDataFrame TIMESTAMP AS OF '2022-07-14T18:45:46.000Z'

# COMMAND ----------

# DBTITLE 1,Query an "Older Snapshot" of a "Delta Table", Defined by "Path, Using SQL "TIMESTAMP AS OF" Syntax
# MAGIC %sql
# MAGIC SELECT * FROM delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath` TIMESTAMP AS OF '2022-07-14T19:06:41.000Z'

# COMMAND ----------

# DBTITLE 1,Query an "Older Snapshot" of a "Delta Table", Defined in the "Metastore", Using DataFrameReader Option "timestampAsOf"
# "DataFrameReader Options" allow to create a "DataFrame" from a "Delta Table" that is "Fixed" to a specific "Version" of the "Delta Table".
timestamp_string = '2022-07-14T18:45:46.000Z'
df_TimeTravel = spark.read.format("delta").option("timestampAsOf", timestamp_string).table("retailer_db.tbl_DeltaCustomerFromDataFrame")
display(df_TimeTravel)

# COMMAND ----------

# DBTITLE 1,Query an "Older Snapshot" of a "Delta Table", Defined by "Path, Using DataFrameReader Option "timestampAsOf"
timestamp_string = '2022-07-14T19:06:41.000Z'
df_TimeTravel = spark.read.format("delta").option("timestampAsOf", timestamp_string).load("/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath")
display(df_TimeTravel)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Time Travel" Using "VERSION AS OF"
# MAGIC * "Version" is a "Long" Value that can be obtained from the "Output" of "DESCRIBE HISTORY".

# COMMAND ----------

# DBTITLE 1,Query an "Older Snapshot" of a "Delta Table", Defined in the "Metastore", Using SQL "VERSION AS OF" Syntax
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_DeltaCustomerFromDataFrame VERSION AS OF 0

# COMMAND ----------

# DBTITLE 1,Query an "Older Snapshot" of a "Delta Table", Defined by "Path, Using SQL "VERSION AS OF" Syntax
# MAGIC %sql
# MAGIC SELECT * FROM delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath` VERSION AS OF 0

# COMMAND ----------

# DBTITLE 1,Query an "Older Snapshot" of a "Delta Table", Defined in the "Metastore", Using DataFrameReader Option "versionAsOf"
version = 0
df_TimeTravel = spark.read.format("delta").option("versionAsOf", version).table("retailer_db.tbl_DeltaCustomerFromDataFrame")
display(df_TimeTravel)

# COMMAND ----------

# DBTITLE 1,Query an "Older Snapshot" of a "Delta Table", Defined by "Path, Using DataFrameReader Option "versionAsOf"
version = 0
df_TimeTravel = spark.read.format("delta").option("versionAsOf", version).load("/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath")
display(df_TimeTravel)

# COMMAND ----------

# MAGIC %md
# MAGIC # How to Get the Latest Version of a Delta Table
# MAGIC * A common pattern is to use the "Latest State" of the "Delta Table" throughout the "Execution" of an "Azure Databricks Job" to update "Downstream Applications".
# MAGIC * Because "Delta Tables" "Auto Update", a "DataFrame" loaded from a "Delta Table" may return different results across invocations if the "Underlying Data" is "Updated". By using "Time Travel", it is possible to "Fix" the "Data" returned by the "DataFrame" across invocations.

# COMMAND ----------

# DBTITLE 1,Get the "Latest Version" of a "Delta Table", Defined in the "Metastore"
latestVersion = spark.sql("SELECT MAX(version) FROM (DESCRIBE HISTORY retailer_db.tbl_DeltaCustomerFromDataFrame)").collect()
df_LoadLatestVersion = spark.read.format("delta").option("versionAsOf", latestVersion[0][0]).table("retailer_db.tbl_DeltaCustomerFromDataFrame")
display(df_LoadLatestVersion)

# COMMAND ----------

# DBTITLE 1,Get the "Latest Version" of a "Delta Table", Defined by "Path"
latestVersion = spark.sql("SELECT MAX(version) FROM (DESCRIBE HISTORY delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`)").collect()
df_LoadLatestVersion = spark.read.format("delta").option("versionAsOf", latestVersion[0][0]).load("/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath")
display(df_LoadLatestVersion)

# COMMAND ----------

# MAGIC %md
# MAGIC # Using "@" Syntax on a Delta Table for Time Travel
# MAGIC * There may be a "Parametrized Pipeline", where the "Input Path" of the "Pipeline" is a "Parameter" of the "Job". After the "Execution" of the "Job", it is possible to reproduce the "Output" some time in the future. In this case, use the "@" Syntax to specify the "Timestamp" or "Version".
# MAGIC * A. The "Timestamp" must be in "yyyyMMddHHmmssSSS" Format.
# MAGIC * B. It is possible to specify a "Version" after "@" by prepending a "v" to the "Version".

# COMMAND ----------

# DBTITLE 1,Use "@" Syntax to Specify the "Timestamp" on a "Delta Table", Defined in the "Metastore", Using Spark SQL
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_DeltaCustomerFromDataFrame@20220714184546000

# COMMAND ----------

# DBTITLE 1,Use "@" Syntax to Specify the "Timestamp" on a "Delta Table", Defined by "Path", Using Spark SQL
# MAGIC %sql
# MAGIC SELECT * FROM delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath@20220714190641000`

# COMMAND ----------

# DBTITLE 1,Use "@" Syntax to Specify the "Timestamp" on a "Delta Table", Defined in the "Metastore", Using DataFrameReader
df_TimestampOfDeltaTable = spark.read.format("delta").table("retailer_db.tbl_DeltaCustomerFromDataFrame@20220714184546000")
display(df_TimestampOfDeltaTable)

# COMMAND ----------

# DBTITLE 1,Use "@" Syntax to Specify the "Timestamp" on a "Delta Table", Defined by "Path", Using DataFrameReader
df_TimestampOfDeltaTableWithPathOnly = spark.read.format("delta").load("/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath@20220714190641000")
display(df_TimestampOfDeltaTableWithPathOnly)

# COMMAND ----------

# DBTITLE 1,Use "@" Syntax to Specify the "Version" on a "Delta Table", Defined in the "Metastore", Using Spark SQL
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_DeltaCustomerFromDataFrame@v0

# COMMAND ----------

# DBTITLE 1,Use "@" Syntax to Specify the "Version" on a "Delta Table", Defined by "Path", Using Spark SQL
# MAGIC %sql
# MAGIC SELECT * FROM delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath@v0`

# COMMAND ----------

# DBTITLE 1,Use "@" Syntax to Specify the "Version" on a "Delta Table", Defined in the "Metastore", Using DataFrameReader
df_VersionOfDeltaTable = spark.read.format("delta").table("retailer_db.tbl_DeltaCustomerFromDataFrame@v0")
display(df_VersionOfDeltaTable)

# COMMAND ----------

# DBTITLE 1,Use "@" Syntax to Specify the "Version" on a "Delta Table", Defined by "Path", Using DataFrameReader
df_VersionOfDeltaTableWithPathOnly = spark.read.format("delta").load("/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath@v0")
display(df_VersionOfDeltaTableWithPathOnly)
