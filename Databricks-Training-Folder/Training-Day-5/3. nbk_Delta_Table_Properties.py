# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 5
# MAGIC * Topic: Introduction to "Delta Table Properties".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Delta Table Properties"
# MAGIC * "Delta Lake" "Reserves" the "Delta Table Properties" that are "Starting" with "<b>delta.</b>".
# MAGIC <br>These "Properties" may have "Specific Meanings", and "Affect" the "Behaviors" of the "Delta Tables" when these "Properties" are "Set".

# COMMAND ----------

# MAGIC %md
# MAGIC # Default Properties of Delta Table
# MAGIC * "Configurations" of the "Delta Tables" that are "Set" in the "Spark Session" actually "Override" the "Default Properties" of the "Delta Table" for the "New Delta Tables" that are "Created" in that "Spark Session".
# MAGIC <br>The "Prefix" that is "Used" in the "Spark Session" is "Different" from the "Configurations" that are "Used" in the "Properties" of the "Delta Table".
# MAGIC * Example: To "Set" the Property "<b>delta.appendOnly</b>" to "<b>true</b>" for "All" the "New Delta Tables" that are "Created" in a "Spark Session", the following "Spark Session" Property is "Set" -
# MAGIC <br><b>spark.conf.set("spark.databricks.delta.properties.defaults.appendOnly", "true")</b>
# MAGIC * To "Modify" the "Table Properties" of the "Existing Delta Tables", the "SET TBLPROPERTIES" are "Used".

# COMMAND ----------

# MAGIC %md
# MAGIC # delta.appendOnly
# MAGIC * If a "Delta Table" needs to be "<b>Append-Only</b>", then the Property "<b>delta.appendOnly</b>" needs to be "Set" to "<b>true</b>".
# MAGIC * If a "Delta Table" is "Append-Only", then the "Existing Records" "Cannot" be "Deleted", and the "Existing Values" "Cannot" be "Updated".
# MAGIC * The "Default Value" for the Property "<b>delta.appendOnly</b>" is "<b>false</b>".

# COMMAND ----------

# DBTITLE 1,Using "Spark Session" Configuration for "All" the "New Delta Tables" "Created" in a "Spark Session"
spark.conf.set("spark.databricks.delta.properties.defaults.appendOnly", "true")

# COMMAND ----------

# DBTITLE 1,Using "SET TBLPROPERTIES" to "Modify" the "Table Properties" of the "Existing Delta Tables"
# MAGIC %sql
# MAGIC ALTER TABLE retailer_db.tbl_DeltaCustomer 
# MAGIC SET TBLPROPERTIES (delta.appendOnly = true);

# COMMAND ----------

# MAGIC %md
# MAGIC # delta.autoOptimize.autoCompact
# MAGIC * For "Delta Lake" to "Automatically Optimize" the "Layout" of the "Files" for a "Delta Table", the Property "<b>delta.autoOptimize.autoCompact</b>" needs to be "Set" to "<b>true</b>".

# COMMAND ----------

# DBTITLE 1,Using "Spark Session" Configuration for "All" the "New Delta Tables" "Created" in a "Spark Session"
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")

# COMMAND ----------

# DBTITLE 1,Using "SET TBLPROPERTIES" to "Modify" the "Table Properties" of the "Existing Delta Tables"
# MAGIC %sql
# MAGIC ALTER TABLE retailer_db.tbl_DeltaCustomer 
# MAGIC SET TBLPROPERTIES (delta.autoOptimize.autoCompact = true);

# COMMAND ----------

# MAGIC %md
# MAGIC # delta.autoOptimize.optimizeWrite
# MAGIC * For "Delta Lake" to "Automatically Optimize" the "Layout" of the "Files" for a "Delta Table" during "<b>Writes</b>", the Property "<b>delta.autoOptimize.optimizeWrite</b>" needs to be "Set" to "<b>true</b>".

# COMMAND ----------

# DBTITLE 1,Using "Spark Session" Configuration for "All" the "New Delta Tables" "Created" in a "Spark Session"
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")

# COMMAND ----------

# DBTITLE 1,Using "SET TBLPROPERTIES" to "Modify" the "Table Properties" of the "Existing Delta Tables"
# MAGIC %sql
# MAGIC ALTER TABLE retailer_db.tbl_DeltaCustomer 
# MAGIC SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

# COMMAND ----------

# MAGIC %md
# MAGIC # delta.checkpoint.writeStatsAsJson
# MAGIC * For "Delta Lake" to "Write" the "File Statistics" in "Check Points" in the "<b>JSON</b>" Format for the "stats" Column, the Property "<b>delta.checkpoint.writeStatsAsJson</b>" needs to be "Set" to "<b>true</b>".
# MAGIC * The "Default Value" for the Property "<b>delta.checkpoint.writeStatsAsJson</b>" is "<b>true</b>".

# COMMAND ----------

# DBTITLE 1,Using "Spark Session" Configuration for "All" the "New Delta Tables" "Created" in a "Spark Session"
spark.conf.set("spark.databricks.delta.properties.defaults.checkpoint.writeStatsAsJson", "true")

# COMMAND ----------

# DBTITLE 1,Using "SET TBLPROPERTIES" to "Modify" the "Table Properties" of the "Existing Delta Tables"
# MAGIC %sql
# MAGIC ALTER TABLE retailer_db.tbl_DeltaCustomer 
# MAGIC SET TBLPROPERTIES (delta.checkpoint.writeStatsAsJson = true);

# COMMAND ----------

# MAGIC %md
# MAGIC # delta.checkpoint.writeStatsAsStruct
# MAGIC * For "Delta Lake" to "Write" the "File Statistics" in "Check Points" in the "<b>Struct</b>" Format for the "<b>stats_parsed</b>" Column and to "Write" the "Partition Values" as a "Struct" for "<b>partitionValues_parsed</b>", the Property "<b>delta.checkpoint.writeStatsAsStruct</b>" needs to be "Set" to "<b>true</b>".

# COMMAND ----------

# DBTITLE 1,Using "Spark Session" Configuration for "All" the "New Delta Tables" "Created" in a "Spark Session"
spark.conf.set("spark.databricks.delta.properties.defaults.checkpoint.writeStatsAsStruct", "true")

# COMMAND ----------

# DBTITLE 1,Using "SET TBLPROPERTIES" to "Modify" the "Table Properties" of the "Existing Delta Tables"
# MAGIC %sql
# MAGIC ALTER TABLE retailer_db.tbl_DeltaCustomer 
# MAGIC SET TBLPROPERTIES (delta.checkpoint.writeStatsAsStruct = true);

# COMMAND ----------

# MAGIC %md
# MAGIC # delta.columnMapping.mode
# MAGIC * "Azure Databricks" "Supports" the "<b>Column Mapping</b>" for "Delta Tables", which "Allows" the "Columns" of the "Delta Table" and the "Columns" of the "Corresponding Parquet Files" to "Use" "Different Names".
# MAGIC * "<b>Column Mapping</b>" "Enables" the "Delta Schema Evolution Operations", such as "RENAME COLUMN" on a "Delta Table" "Without" the "Need" to "Re-Write" the "Underlying Parquet Files".
# MAGIC * "<b>Column Mapping</b>" also "Allows" the "Users" to "Name" the "Columns" of a "Delta Table" by "Using" the "Characters" that are "Not Allowed" by the "Parquet", such as "Spaces", so that "Users" can "Directly Ingest" the "CSV" or "JSON" Data into the "Delta Table" "Without" the "Need" to "Rename" the "Columns" due to "Previous Character Constraints".
# MAGIC * To "Enable" the "<b>Column Mapping</b>" to "Make Sure" that the "Columns" of a "Delta Table" and the "Columns" of the "Corresponding Parquet Files" can "Use" "Different Names", the Property "<b>delta.columnMapping.mode</b>" needs to be "Set" to "<b>true</b>".

# COMMAND ----------

# DBTITLE 1,Using "Spark Session" Configuration for "All" the "New Delta Tables" "Created" in a "Spark Session"
spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode", "true")

# COMMAND ----------

# DBTITLE 1,Using "SET TBLPROPERTIES" to "Modify" the "Table Properties" of the "Existing Delta Tables"
# MAGIC %sql
# MAGIC ALTER TABLE retailer_db.tbl_DeltaCustomer 
# MAGIC SET TBLPROPERTIES (delta.columnMapping.mode = true);
