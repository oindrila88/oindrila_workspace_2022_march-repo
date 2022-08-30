# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 4
# MAGIC * Topic: How to Use "Schema Evolution" Using "Auto Loader"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema Evolution in Auto Loader
# MAGIC * "Auto Loader" "Detects" the "Addition" of "New Columns" as it "Processes" the Data.
# MAGIC * By default, "Addition" of a "New Column" will "Cause" the "Streams" to "Stop" with an "UnknownFieldException". "Before" the "Stream" "Throws" this "Error", "Auto Loader" "Performs" the "Schema Inference" on the "Latest Micro-Batch" of the Data, and "Updates" the "Schema Location" with the "Latest Schema".
# MAGIC * "New Columns" are "Merged" to the "End" of the "Schema".
# MAGIC * The "Data Types" of the "Existing Columns" Remain "Unchanged".
# MAGIC * By "Setting" the "Auto Loader Stream" "Within" a "Databricks Job", it is possible to "Restart" the "Stream" "Automatically", "After" such "Schema Changes".
# MAGIC * "Auto Loader" "Supports" the following "Modes" for "Schema Evolution", which can be "Set" in the Option "<b>cloudFiles.schemaEvolutionMode</b>":
# MAGIC * A. <b>addNewColumns</b>
# MAGIC * B. <b>failOnNewColumns</b>
# MAGIC * C. <b>rescue</b>
# MAGIC * D. <b>none</b>

# COMMAND ----------

# MAGIC %md
# MAGIC # "Schema Evolution" Using "addNewColumns" Mode in "Auto Loader"
# MAGIC * The "Default Mode" When a "Schema" is "Not Provided" to "Auto Loader" is "<b>addNewColumns</b>".
# MAGIC * The "Streaming Job" will "Fail" with an "UnknownFieldException".
# MAGIC * "New Columns" are "Added" to the "Schema". "Existing Columns" "Do Not Evolve" the "Data Types".
# MAGIC * "addNewColumns" Mode is "Not Allowed" When the "Schema" of the "Stream" is "Provided". Provide the "Schema" as a "Schema Hint" instead, if the "addNewColumns" Mode needs to be Used.

# COMMAND ----------

# MAGIC %md
# MAGIC # "Schema Evolution" Using "failOnNewColumns" Mode in "Auto Loader"
# MAGIC * If "Auto Loader" "Detects" a "New Column", the "Stream" will "Fail". It will "Not Restart" "Unless" the "Provided Schema" is "Updated", or the "Offending Data File" is "Removed".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Schema Evolution" Using "rescue" Mode in "Auto Loader"
# MAGIC * The "Stream" "Runs" with the Very First "Inferred" or "Provided" "Schema".
# MAGIC * Any "Data Type Changes" or "New Columns" that are "Added" are "Rescued" in the "Rescued Data Column" that is "Automatically Added" to the "Stream's Schema" as "_rescued_data".
# MAGIC * In this "Mode", the "Stream" will "Not Fail" due to the "Schema Changes".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Schema Evolution" Using "none" Mode in "Auto Loader"
# MAGIC * The "Default Mode" When a "Schema" is "Provided" to "Auto Loader" is "<b>none</b>".
# MAGIC * In this "Mode" -
# MAGIC * A. The "Schema" Does "Not Evolve".
# MAGIC * B. "New Columns" are "Ignored".
# MAGIC * C. The "Data" is "Not Rescued" Unless the "Rescued Data Column" is "Provided Separately" as an Option.

# COMMAND ----------

# DBTITLE 1,"Read" the Data from the "Continuous Arriving CSV Files" from a "Directory" in "ADLS Gen2"
from pyspark.sql.types import *

sourceFileLocation = '/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-schema-inference-csv-files'
sourceFileFormat = 'CSV'
schemaLocationPath = '/mnt/with-aad-app/databricks-training-folder/day-4/target-delta-table/autoloader_customers/checkpoint-and-schema-location'
rescuedDataColumnName = '_rescued_data_column'

# Read the "Continuous Arriving CSV Files" from the Mounted Path "/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-schema-inference-csv-files" Using "Auto Loader".
df_ReadCustomersCsvFiles = spark.readStream.format("cloudFiles")\
                                           .option("cloudFiles.format", sourceFileFormat)\
                                           .option("cloudFiles.rescuedDataColumn", rescuedDataColumnName)\
                                           .option("header", "true")\
                                           .option("cloudFiles.schemaLocation", schemaLocationPath)\
                                           .option("cloudFiles.inferColumnTypes", "true")\
                                           .option("cloudFiles.schemaEvolutionMode", "none")\
                                           .load(sourceFileLocation)

# COMMAND ----------

# DBTITLE 1,Display the "Data" of the "Streaming DataFrame"
display(df_ReadCustomersCsvFiles)
