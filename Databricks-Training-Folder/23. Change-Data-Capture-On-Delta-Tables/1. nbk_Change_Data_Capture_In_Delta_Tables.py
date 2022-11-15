# Databricks notebook source
# MAGIC %md
# MAGIC # What is "Change Data Capture" ("CDC") in Databricks
# MAGIC * Topic: Introduction to "Change Data Capture" ("CDC")
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Change Data Capture"?
# MAGIC * "Change Data Capture", or "CDC", in short, "Refers" to the "Process" of "Capturing" "Only" the "Changes" made in a "Set of Data Sources" since the "Last Successful Load" and "Merging" "Only" those "Changes" in a "Set of Target Tables", typically in a "Data Warehouse".
# MAGIC * The "Data Warehouse" is typically "Refreshed" "Nightly", "Hourly", or, in some cases, "Sub-Hourly" (e.g., "Every 15 Minutes"). This "Period" is "Refered" to as the "Refresh Period".
# MAGIC * The "Set" of "Changed Records" for a given "Table" "Within" a "Refresh Period" is "Referred" to as a "Change Set".
# MAGIC * Finally, the "Set of Records" "Within" a "Change Set" that has the "Same" "Primary Key" is "Referred" to as a "Record Set". Intuitively the "Record Set" "Refer" to "Different" "Changes" for the "Same Record" in the "Final Table".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Medallion Architecture"?
# MAGIC * Typically "CDC" is "Used" in an "Ingestion" to "Analytics Architecture", called the "Medallion Architecture".
# MAGIC * The "Medallion Architecture" takes "Raw Data" "Landed" from the "Source Systems" and "Refines" the "Data" through the "Bronze", "Silver" and "Gold" "Tables".
# MAGIC * "CDC" and the "Medallion Architecture" provide "Multiple Benefits" to "Users" since the "Only Changed" or "Added Data" needs to be "Processed".
# MAGIC * In addition, the "Different Tables" in the "Architecture" "Allow" the "Different Personas", such as "Data Scientists" and "BI Analysts", to "Use" the "Correct Up-To-Date Data" for their "Needs".
# MAGIC <br><img src = '/files/tables/images/cdc_1.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Change Data Feed"?
# MAGIC * "Change Data Feed" ("CDF") feature "Allows" the "Delta Tables" to "Track" the "Row-Level Changes" between the "Versions" of a "Delta Table".
# MAGIC * With "Change Data Feed" ("CDF") "Enabled" on a "Delta Table", the "Databricks Runtime" "Records" the "Change Events" for "All" the "Data" "Written" into the "Delta Table". This "Includes" the "Row" of "Data" "Along With" the "Metadata" "Indicating" whether the "Specified Row" was "Inserted", "Deleted", or "Updated".
# MAGIC * It is possible to "Read" the "Change Events" in the "Batch Queries" using the "SQL" and "DataFrame APIs", i.e., "df.read".
# MAGIC * It is possible to "Read" the "Change Events" in the "Streaming Queries" using the "DataFrame APIs", i.e., "df.readStream".
# MAGIC * "CDF" "Captures" the "Changes Only" from a "Delta Table" and is "Only Forward-Looking" "Once Enabled".
# MAGIC * Only the "Changes Made" After the "Change Data Feed" is "Enabled" on a "Delta Table" are "Recorded". "Past Changes" to a "Delta Table" are "Not Captured".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Use cases" of "Change Data Feed"
# MAGIC * "Change Data Feed" is "Not Enabled" "By Default". The following "Use Cases" "Drive" "When" the "Change Data Feed"should be "Enabled".
# MAGIC * A. <b>Silver and Gold Tables</b>: "Improve" the "Delta Performance" by "Processing" the "Only Row-Level Changes" following "Initial" "MERGE", "UPDATE", or "DELETE" Operations to "Accelerate" and "Simplify" the "ETL" and "ELT" Operations.
# MAGIC * B. <b>Materialized Views</b>: "Create" the "Up-To-Date", "Aggregated Views" of "Information" for "Use" in "BI" and "Analytics" "Without" having to "Re-Process" the "Full Underlying Tables", Instead "Updating" only where "Changes" have "Come Through".
# MAGIC * C. <b>Transmit Changes</b>: "Send" a "Change Data Feed" to "Downstream Systems", such as "Kafka" or "RDBMS" that can "Use" it to "Incrementally Process" in the "Later Stages" of "Data Pipelines".
# MAGIC * D. <b>Audit Trail Table</b>: "Capture" the "Change Data Feed" as a "Delta Table" provides "Perpetual Storage" and "Efficient Query Capability" to "See" "All" the "Changes" "Over Time", including when "Deletes" "Occur" and what "Updates" were "Made".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Enable" the "Change Data Feed" on "New Table"
# MAGIC * The "Change Data Feed" Option must be "Explicitly Enabled" on a "New Table" by "Using" the "Table Property" "<b>delta.enableChangeDataFeed = true</b>" in the "CREATE TABLE" Command.

# COMMAND ----------

# DBTITLE 1,"Create" a "New Table", i.e., "retailer_db.tbl_StudentWithCdcEnabled" With "CDC Enabled"
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS retailer_db.tbl_StudentWithCdcEnabled;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS retailer_db.tbl_StudentWithCdcEnabled
# MAGIC (
# MAGIC     FirstName STRING,
# MAGIC     MiddleName STRING,
# MAGIC     LastName STRING,
# MAGIC     Subject STRING,
# MAGIC     Marks INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# DBTITLE 1,"Read" the Data from the "Continuous Arriving CSV Files" from a "Directory" in "ADLS Gen2"
from pyspark.sql.types import *

sourceFileLocation = '/mnt/with-aad-app/databricks-training-folder/day-3/autoloader-csv-files/'
sourceFileFormat = 'CSV'
targetDeltaTablePath = '/mnt/with-aad-app/databricks-training-folder/day-3/target-delta-table/foreachBatch-and-cdc-enabled-delta-students/'
checkpointDirectoryPath = '/mnt/with-aad-app/databricks-training-folder/day-3/target-delta-table/autoloader-checkpoint-directory/foreachBatch-and-cdc-enabled-delta-students/'
targetDeltaTableName = 'retailer_db.tbl_StudentWithCdcEnabled'
appId = 'foreachBatch-and-cdc-enabled-delta-students-example'

# Create a "Schema" of the Data to be "Loaded" from the "Continuous Arriving CSV Files".
studentsDataSchema = StructType([
    StructField("FirstName", StringType(), False),
    StructField("MiddleName", StringType(), True),
    StructField("LastName", StringType(), False),
    StructField("Subject", StringType(), False),
    StructField("Marks", IntegerType(), False)
])

# Read the "Continuous Arriving CSV Files" from the Mounted Path "/mnt/with-aad-app/databricks-training-folder/day-3/autoloader-csv-files" Using "Auto Loader".
df_ReadStudentCsvFiles = spark.readStream.format("cloudFiles")\
                                         .option("cloudFiles.format", sourceFileFormat)\
                                         .option("header", "true")\
                                         .schema(studentsDataSchema)\
                                         .load(sourceFileLocation)

# COMMAND ----------

# DBTITLE 1,Create the "Function" to be "Called" from the "foreachBatch()" on "Each Micro-Batch Output"
def processData(df_streamOutput, batchId):
    try:
        df_streamOutput.write\
                       .format("delta")\
                       .mode("append")\
                       .option("mergeSchema", "true")\
                       .option("txnVersion", batchId)\
                       .option("txnAppId", appId)\
                       .option("path", targetDeltaTablePath)\
                       .saveAsTable(targetDeltaTableName)
    except Exception as e:
        raise e

# COMMAND ----------

# DBTITLE 1,"Store" the Data from the "Streaming DataFrame" to the "Delta Table"
df_ReadStudentCsvFiles.writeStream\
                      .format("delta")\
                      .outputMode("append")\
                      .option("checkpointLocation", checkpointDirectoryPath)\
                      .queryName(f"Running Auto Loader for Table '{targetDeltaTableName}'")\
                      .trigger(availableNow = True)\
                      .foreachBatch(processData)\
                      .start()

# COMMAND ----------

# DBTITLE 1,Display the Data of the Delta Table "retailer_db.tbl_StudentWithCdcEnabled"
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_StudentWithCdcEnabled;

# COMMAND ----------

# MAGIC %md
# MAGIC # "Enable" the "Change Data Feed" on "Existing Table"
# MAGIC * The "Change Data Feed" Option must be "Explicitly Enabled" on an "Existing Table" by "Using" the "Table Property" "<b>delta.enableChangeDataFeed = true</b>" in the "ALTER TABLE" Command.

# COMMAND ----------

# DBTITLE 1,"Enable CDC" for the "Already Existing Table", i.e., "retailer_db.tbl_CustomerWithDeltaFile"
# MAGIC %sql
# MAGIC ALTER TABLE retailer_db.tbl_DeltaStudentWithForeachBatch
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Enable" the "Change Data Feed" on "All New Tables"
# MAGIC * The "Change Data Feed" Option must be "Explicitly Enabled" on "All New Tables" by "Using" the "Spark Configuration" "<b>spark.databricks.delta.properties.defaults.enableChangeDataFeed = true</b>" in the "Cluster" Level, or, in "SparkSession".

# COMMAND ----------

# DBTITLE 1,"Enable CDC" for the "Current SparkSession"
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Change Data Storage"?
# MAGIC * "Databricks" "Records" the "Change in Data" for "UPDATE", "DELETE", and "MERGE" Operations in the "_change_data" Folder "Under" the "Delta Table" Directory.
# MAGIC * These "Records" may be "Skipped" when "Databricks" "Detects" that the "Change Data Feed" can be Efficiently "Computed Directly" from the "Transaction Log". In particular, the "Insert-Only" Operations and the "Full Partition Deletes" will "Not Generate" any "Data" in the "_change_data" Directory.
# MAGIC * The "Files" in the "_change_data" Folder "Follow" the "Retention Policy" of the "Table". Therefore, if the "VACUUM"  Command is "Run" on the "Table", the "Change Data Feed" Data is also "Deleted".

# COMMAND ----------

# MAGIC %md
# MAGIC # Change Data Event Schema
# MAGIC * In "Addition" to the "Data Columns", the "Change Data" "Contains" the "Metadata Columns" that "Identify" the "Type" of the "Change Event".
# MAGIC * A. <b>_change_type</b>: This Column is of Data Type "String". The "Values" can be - "insert", "update_preimage", "update_postimage", or, "delete".
# MAGIC * "preimage" is the "Value" that is "Before" the "Update".
# MAGIC * "postimage" is the "Value" that is "After" the "Update".
# MAGIC * B. <b>_commit_version</b>: This Column is of Data Type "Long". The "Value" is the "Delta Log", or, the "Table Version" "Containing" the "Change".
# MAGIC * C. <b>_commit_timestamp</b>: This Column is of Data Type "Timestamp". The "Value" is the "Timestamp" that is "Associated" when the "Commit" was "Created".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Read" the "Changes" in "Batch Queries"
# MAGIC * It is possible to "Provide" either the "Version" or the "Timestamp" for the "Start" and "End".
# MAGIC * The "Start" and "End" "Versions" and the "Timestamps" are "Inclusive" in the "Queries".
# MAGIC * To "Read" the "Changes" from a Particular "Start Version" to the "Latest Version" of the "Table", "Specify" Only the "Starting Version" or "Starting Timestamp".
# MAGIC * The "Version" must be "Specified" as an "Integer" and the "Timestamps" must be "Specified" as a "String" in the Format "yyyy-MM-dd[ HH:mm:ss[.SSS]]".
# MAGIC * If you provide a version lower or timestamp older than one that has recorded change events, that is, when the change data feed was enabled, an error is thrown indicating that the change data feed was not enabled.

# COMMAND ----------

# DBTITLE 1,Read the "Changes" from "Version" "0" to "1" Using "Spark SQL"
# MAGIC %sql
# MAGIC SELECT * FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 3);

# COMMAND ----------

# DBTITLE 1,Read the "Changes" With "Timestamp" as "String-Formatted Timestamps" Using "Spark SQL"
# MAGIC %sql
# MAGIC SELECT * FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', '2022-08-05T21:26:40.000', '2022-09-05T21:26:40.000');
# MAGIC 
# MAGIC -- The "endTimestamp" has to be any "Existing Value" from the Column "_commit_timestamp".

# COMMAND ----------

# DBTITLE 1,Read the "Changes" by "Providing" Only the "Starting Version"
# MAGIC %sql
# MAGIC SELECT * FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0);

# COMMAND ----------

# MAGIC %md
# MAGIC # "Read" the "Changes" in "Streaming Queries"
# MAGIC * To "Get" the "Change Data" While "Reading" the "Delta Table", the Option "readChangeFeed" "Must" be "Set" to "True".
# MAGIC * The "startingVersion" or "startingTimestamp" are "Optional" and "If Not Provided", the "Stream" "Returns" the "Latest Snapshot" of the "Delta Table" at the "Time" of "Streaming" as an "INSERT" and "Future Changes" as "Change Data".
# MAGIC * "Options" like the "Rate Limits" ("maxFilesPerTrigger", "maxBytesPerTrigger") and "excludeRegex" are "Also Supported" When "Reading" the "Change Data".
# MAGIC * "Rate Limits" can be "Atomic" for "Versions" "Other Than" the "Starting Snapshot Version". That is, the "Entire Commit Version" will be "Rate Limited" or the "Entire Commit" will be "Returned".

# COMMAND ----------

# DBTITLE 1,Read the "Changes" by "Providing" a "Starting Version" Using "Python"
display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingVersion", 0)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# DBTITLE 1,Read the "Changes" by "Providing" a "Starting Timestamp" as "String-Formatted Timestamp" Using "Python"
display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingTimestamp", '2022-09-05T21:26:40.000')\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# DBTITLE 1,"Latest Snapshot" Getting "Fetched"
# "Not Providing" a "Starting Version"/"Starting Timestamp" will "Result" in the "Latest Snapshot" being "Fetched First".
display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# DBTITLE 1,Get the "Version" of the "Last Commit"
# MAGIC %sql
# MAGIC SELECT MAX(_commit_version) LAST_COMMIT_VERSION FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0);

# COMMAND ----------

# DBTITLE 1,Get the "Timestamp" of the "Last Commit"
# MAGIC %sql
# MAGIC SELECT MAX(_commit_timestamp) LAST_COMMIT_TIMESTAMP FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0);

# COMMAND ----------

# MAGIC %md
# MAGIC # "Error" is "Thrown" When "Starting Version" or "Starting Timestamp" "Exceeds" the "Last Commit" on a "Delta Table"
# MAGIC * By Default, if a "User" "Passes" in a "Version" or "Timestamp" "Exceeding" the "Last Commit" on a "Delta Table", the "Errors" "VersionNotFoundException", or, "timestampGreaterThanLatestCommit" will be "Thrown".
# MAGIC * "CDF" can "Handle" the "Out of Range" "Version Case", if the user sets the "Configuration" "<b>spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled</b>" to "<b>True</b>".

# COMMAND ----------

# DBTITLE 1,"Starting Version" is "Greater Than" the "Last Commit" - "VersionNotFoundException" is "Thrown"
endVersion = spark.sql("SELECT MAX(_commit_version) FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0)").collect()[0][0]
startVersion = endVersion + 2

display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingVersion", startVersion)\
                        .option("endingVersion", endVersion)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# DBTITLE 1,"Starting Timestamp" is "Newer Than" the "Last Commit" - "timestampGreaterThanLatestCommit" is "Thrown"
from pyspark.sql.functions import *

df_endTimestamp = spark.sql("SELECT MAX(_commit_timestamp) max_timestamp FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0)")
endTimestamp = df_endTimestamp.collect()[0][0]
df_startTimestamp = df_endTimestamp.withColumn('new_max_timestamp', df_endTimestamp.max_timestamp + expr('INTERVAL 2 HOURS'))
startTimestamp = df_startTimestamp.first()["new_max_timestamp"]

display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingTimestamp", startTimestamp)\
                        .option("endingTimestamp", endTimestamp)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# DBTITLE 1,Both "Starting Version" and "Ending Version" are "Greater Than" the "Last Commit" - "VersionNotFoundException" is "Thrown"
endVersion = spark.sql("SELECT MAX(_commit_version) FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0)").collect()[0][0]
startVersion = endVersion + 1

display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingVersion", startVersion)\
                        .option("endingVersion", endVersion + 3)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# DBTITLE 1,Both "Starting Timestamp" and "Ending Timestamp" are "Newer Than" the "Last Commit" - "timestampGreaterThanLatestCommit" is "Thrown"
from pyspark.sql.functions import *

df_endTimestamp = spark.sql("SELECT MAX(_commit_timestamp) max_timestamp FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0)")
df_endTimestamp = df_endTimestamp.withColumn('end_timestamp', df_endTimestamp.max_timestamp + expr('INTERVAL 2 HOURS'))
endTimestamp = df_endTimestamp.first()["end_timestamp"]
df_startTimestamp = df_endTimestamp.withColumn('new_max_timestamp', df_endTimestamp.max_timestamp + expr('INTERVAL 4 HOURS'))
startTimestamp = df_startTimestamp.first()["new_max_timestamp"]

display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingTimestamp", startTimestamp)\
                        .option("endingTimestamp", endTimestamp)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# DBTITLE 1,Only "Ending Version" is "Greater Than" the "Last Commit" - "No Error Thrown"
# "All" the "Changes" "Between" the "Start Version" and the "Last Commit" are "Returned", if the "Ending Version" is "Greater Than" the "Last Commit".
startVersion = 0
endVersion = spark.sql("SELECT MAX(_commit_version) FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0)").collect()[0][0]

display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingVersion", startVersion)\
                        .option("endingVersion", endVersion + 3)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# DBTITLE 1,Only "Ending Timestamp" is "Newer Than" the "Last Commit" - "No Error Thrown"
# "All" the "Changes" "Between" the "Start Timestamp" and the "Last Commit" are "Returned", if the "Ending Timestamp" is "Newer Than" the "Last Commit".
from pyspark.sql.functions import *

startTimestamp = '2022-09-05T21:26:40.000'
df_endTimestamp = spark.sql("SELECT MAX(_commit_timestamp) max_timestamp FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0)")
df_endTimestamp = df_endTimestamp.withColumn('new_max_timestamp', df_endTimestamp.max_timestamp + expr('INTERVAL 2 HOURS'))
endTimestamp = df_endTimestamp.collect()[0][0]

display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingTimestamp", startTimestamp)\
                        .option("endingTimestamp", endTimestamp)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# MAGIC %md
# MAGIC # "Error" is "Thrown" When "Start Version" or "Start Timestamp" "Exceeds" the "Last Commit" on a "Delta Table" Even With "Spark Configuration"
# MAGIC * If a "Start Version" is "Provided" which is "Greater Than" the "Last Commit" on a "Delta Table", or, a "Start Timestamp" is "Provided" that is "Newer Than" the "Last Commit" on a "Delta Table", then "When" the "Configuration" "<b>spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled</b>" is "Set" to "<b>True</b>", still the "Errors" "VersionNotFoundException", or, "timestampGreaterThanLatestCommit" will be "Thrown"..

# COMMAND ----------

# DBTITLE 1,"Starting Version" is "Greater Than" the "Last Commit" - "VersionNotFoundException" is "Thrown"
spark.conf.set("spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled", True)

endVersion = spark.sql("SELECT MAX(_commit_version) FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0)").collect()[0][0]
startVersion = endVersion + 2

display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingVersion", startVersion)\
                        .option("endingVersion", endVersion)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# DBTITLE 1,"Starting Timestamp" is "Newer Than" the "Last Commit" - "timestampGreaterThanLatestCommit" is "Thrown"
from pyspark.sql.functions import *

spark.conf.set("spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled", True)

df_endTimestamp = spark.sql("SELECT MAX(_commit_timestamp) max_timestamp FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0)")
endTimestamp = df_endTimestamp.collect()[0][0]
df_startTimestamp = df_endTimestamp.withColumn('new_max_timestamp', df_endTimestamp.max_timestamp + expr('INTERVAL 2 HOURS'))
startTimestamp = df_startTimestamp.first()["new_max_timestamp"]

display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingTimestamp", startTimestamp)\
                        .option("endingTimestamp", endTimestamp)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# DBTITLE 1,Both "Starting Version" and "Ending Version" are "Greater Than" the "Last Commit" - "VersionNotFoundException" is "Thrown"
spark.conf.set("spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled", True)

df_endTimestamp = spark.sql("SELECT MAX(_commit_timestamp) max_timestamp FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0)")
df_endTimestamp = df_endTimestamp.withColumn('end_timestamp', df_endTimestamp.max_timestamp + expr('INTERVAL 2 HOURS'))
endTimestamp = df_endTimestamp.first()["end_timestamp"]
df_startTimestamp = df_endTimestamp.withColumn('new_max_timestamp', df_endTimestamp.max_timestamp + expr('INTERVAL 4 HOURS'))
startTimestamp = df_startTimestamp.first()["new_max_timestamp"]

display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingTimestamp", startTimestamp)\
                        .option("endingTimestamp", endTimestamp)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# DBTITLE 1,Both "Starting Timestamp" and "Ending Timestamp" are "Newer Than" the "Last Commit" - "timestampGreaterThanLatestCommit" is "Thrown"
from pyspark.sql.functions import *

spark.conf.set("spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled", True)

df_endTimestamp = spark.sql("SELECT MAX(_commit_timestamp) max_timestamp FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0)")
df_endTimestamp = df_endTimestamp.withColumn('end_timestamp', df_endTimestamp.max_timestamp + expr('INTERVAL 2 HOURS'))
endTimestamp = df_endTimestamp.first()["end_timestamp"]
df_startTimestamp = df_endTimestamp.withColumn('new_max_timestamp', df_endTimestamp.max_timestamp + expr('INTERVAL 4 HOURS'))
startTimestamp = df_startTimestamp.first()["new_max_timestamp"]

display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingTimestamp", startTimestamp)\
                        .option("endingTimestamp", endTimestamp)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# MAGIC %md
# MAGIC # "All" the "Changes" "Between" the "Start Version" and the "Last Commit" are "Returned" When "End Version" or "End Timestamp" "Exceeds" the "Last Commit" on a "Delta Table"
# MAGIC * If an "End Version" is "Provided" which is "Greater Than" the "Last Commit" on a "Delta Table", or, an "End Timestamp" is "Provided" that is "Newer Than" the "Last Commit" on a "Delta Table", then "When" the "Configuration" "<b>spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled</b>" is "Set" to "<b>True</b>" in "<b>Batch Read</b>" Mode, "All" the "Changes" "Between" the "Start Version" and the "Last Commit" are to be "Returned".

# COMMAND ----------

# DBTITLE 1,Only "Ending Version" is "Greater Than" the "Last Commit" - "All" the "Changes" "Between" the "Start Version" and the "Last Commit" are "Returned"
spark.conf.set("spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled", True)

startVersion = 0
endVersion = spark.sql("SELECT MAX(_commit_version) FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0)").collect()[0][0]

display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingVersion", startVersion)\
                        .option("endingVersion", endVersion + 3)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# DBTITLE 1,Only "Ending Timestamp" is "Newer Than" the "Last Commit" - "All" the "Changes" "Between" the "Start Timestamp" and the "Last Commit" are "Returned"
from pyspark.sql.functions import *

spark.conf.set("spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled", True)

startTimestamp = '2022-09-05T21:26:40.000'
df_endTimestamp = spark.sql("SELECT MAX(_commit_timestamp) max_timestamp FROM table_changes('retailer_db.tbl_StudentWithCdcEnabled', 0)")
df_endTimestamp = df_endTimestamp.withColumn('new_max_timestamp', df_endTimestamp.max_timestamp + expr('INTERVAL 2 HOURS'))
endTimestamp = df_endTimestamp.collect()[0][0]

display(spark.readStream.format("delta")\
                        .option("readChangeFeed", "true")\
                        .option("startingTimestamp", startTimestamp)\
                        .option("endingTimestamp", endTimestamp)\
                        .table("retailer_db.tbl_StudentWithCdcEnabled")
       )

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table retailer_db.tbl_CustomerWithCdcEnabled
