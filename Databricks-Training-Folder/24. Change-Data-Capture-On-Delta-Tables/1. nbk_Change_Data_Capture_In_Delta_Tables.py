# Databricks notebook source
# MAGIC %md
# MAGIC # What is "Change Data Capture" ("CDC") in Databricks
# MAGIC * Topic: Introduction to "Change Data Capture" ("CDC")
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Change Data Capture"?
# MAGIC * "<b>Change Data Capture</b>", or "<b>CDC</b>", in short, "<b>Refers</b>" to the "<b>Process</b>" of "<b>Capturing</b>" "<b>Only</b>" the "<b>Changes</b>" made in a "<b>Set of Data Sources</b>" since the "<b>Last Successful Load</b>" and "<b>Merging</b>" "<b>Only</b>" those "<b>Changes</b>" in a "<b>Set of Target Tables</b>", typically in a "<b>Data Warehouse</b>".
# MAGIC * The "<b>Data Warehouse</b>" is typically "<b>Refreshed</b>" "<b>Nightly</b>", "<b>Hourly</b>", or, in some cases, "<b>Sub-Hourly</b>" (e.g., "<b>Every 15 Minutes</b>"). This "<b>Period</b>" is "<b>Refered</b>" to as the "<b>Refresh Period</b>".
# MAGIC * The "<b>Set</b>" of "<b>Changed Records</b>" for a given "<b>Table</b>" "<b>Within</b>" a "<b>Refresh Period</b>" is "<b>Referred</b>" to as a "<b>Change Set</b>".
# MAGIC * Finally, the "<b>Set of Records</b>" "<b>Within</b>" a "<b>Change Set</b>" that has the "<b>Same</b>" "<b>Primary Key</b>" is "<b>Referred</b>" to as a "<b>Record Set</b>". Intuitively the "<b>Record Set</b>" "<b>Refer</b>" to "<b>Different</b>" "<b>Changes</b>" for the "<b>Same Record</b>" in the "<b>Final Table</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Medallion Architecture"?
# MAGIC * Typically "<b>CDC</b>" is "<b>Used</b>" in an "<b>Ingestion</b>" to "<b>Analytics Architecture</b>", called the "<b>Medallion Architecture</b>".
# MAGIC * The "<b>Medallion Architecture</b>" takes "<b>Raw Data</b>" "<b>Landed</b>" from the "<b>Source Systems</b>" and "<b>Refines</b>" the "<b>Data</b>" through the "<b>Bronze</b>", "<b>Silver</b>" and "<b>Gold</b>" "<b>Tables</b>".
# MAGIC * "<b>CDC</b>" and the "<b>Medallion Architecture</b>" provide "<b>Multiple Benefits</b>" to "<b>Users</b>" since the "<b>Only Changed</b>" or "<b>Added Data</b>" needs to be "<b>Processed</b>".
# MAGIC * In addition, the "<b>Different Tables</b>" in the "<b>Architecture</b>" "<b>Allow</b>" the "<b>Different Personas</b>", such as "<b>Data Scientists</b>" and "<b>BI Analysts</b>", to "<b>Use</b>" the "<b>Correct Up-To-Date Data</b>" for their "<b>Needs</b>".
# MAGIC * <img src = '/files/tables/images/cdc_1.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Change Data Feed"?
# MAGIC * "<b>Change Data Feed</b>" ("<b>CDF</b>") feature "<b>Allows</b>" the "<b>Delta Tables</b>" to "<b>Track</b>" the "<b>Row-Level Changes</b>" between the "<b>Versions</b>" of a "<b>Delta Table</b>".
# MAGIC * With "<b>Change Data Feed</b>" ("<b>CDF</b>") "<b>Enabled</b>" on a "<b>Delta Table</b>", the "<b>Databricks Runtime</b>" "<b>Records</b>" the "<b>Change Events</b>" for "<b>All</b>" the "<b>Data</b>" "<b>Written</b>" into the "<b>Delta Table</b>". This "<b>Includes</b>" the "<b>Row</b>" of "<b>Data</b>" "<b>Along With</b>" the "<b>Metadata</b>" "<b>Indicating</b>" whether the "<b>Specified Row</b>" was "<b>Inserted</b>", "<b>Deleted</b>", or "<b>Updated</b>".
# MAGIC * It is possible to "<b>Read</b>" the "<b>Change Events</b>" in the "<b>Batch Queries</b>" using the "<b>SQL</b>" and "<b>DataFrame APIs</b>", i.e., "<b>df.read</b>".
# MAGIC * It is possible to "<b>Read</b>" the "<b>Change Events</b>" in the "<b>Streaming Queries</b>" using the "<b>DataFrame APIs</b>", i.e., "<b>df.readStream</b>".
# MAGIC * "<b>CDF</b>" "<b>Captures</b>" the "<b>Changes Only</b>" from a "<b>Delta Table</b>" and is "<b>Only Forward-Looking</b>" "<b>Once Enabled</b>".
# MAGIC * Only the "<b>Changes Made</b>" After the "<b>Change Data Feed</b>" is "<b>Enabled</b>" on a "<b>Delta Table</b>" are "<b>Recorded</b>". "<b>Past Changes</b>" to a "<b>Delta Table</b>" are "<b>Not Captured</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Use cases" of "Change Data Feed"
# MAGIC * "<b>Change Data Feed</b>" is "<b>Not Enabled</b>" "<b>By Default</b>". The following "<b>Use Cases</b>" "<b>Drive</b>" "<b>When</b>" the "<b>Change Data Feed</b>" should be "<b>Enabled</b>".
# MAGIC * A. <b>Silver and Gold Tables</b> - "<b>Improve</b>" the "<b>Delta Performance</b>" by "<b>Processing</b>" the "<b>Only Row-Level Changes</b>" following "<b>Initial</b>" "<b>MERGE</b>", "<b>UPDATE</b>", or "<b>DELETE</b>" Operations to "<b>Accelerate</b>" and "<b>Simplify</b>" the "<b>ETL</b>" and "<b>ELT</b>" Operations.
# MAGIC * B. <b>Materialized Views</b> - "<b>Create</b>" the "<b>Up-To-Date</b>", "<b>Aggregated Views</b>" of "<b>Information</b>" for "<b>Use</b>" in "<b>BI</b>" and "<b>Analytics</b>" "<b>Without</b>" having to "<b>Re-Process</b>" the "<b>Full Underlying Tables</b>", Instead "<b>Updating</b>" only where "<b>Changes</b>" have "<b>Come Through</b>".
# MAGIC * C. <b>Transmit Changes</b> - "<b>Send</b>" a "<b>Change Data Feed</b>" to "<b>Downstream Systems</b>", such as "<b>Kafka</b>" or "<b>RDBMS</b>" that can "<b>Use</b>" it to "<b>Incrementally Process</b>" in the "<b>Later Stages</b>" of "<b>Data Pipelines</b>".
# MAGIC * D. <b>Audit Trail Table</b> - "<b>Capture</b>" the "<b>Change Data Feed</b>" as a "<b>Delta Table</b>" provides "<b>Perpetual Storage</b>" and "<b>Efficient Query Capability</b>" to "<b>See</b>" "<b>All</b>" the "<b>Changes</b>" "<b>Over Time</b>", including when "<b>Deletes</b>" "<b>Occur</b>" and what "<b>Updates</b>" were "<b>Made</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Enable" the "Change Data Feed" on "New Table"
# MAGIC * The "<b>Change Data Feed</b>" Option must be "<b>Explicitly Enabled</b>" on a "<b>New Table</b>" by "<b>Using</b>" the "<b>Table Property</b>" "<b>delta.enableChangeDataFeed = true</b>" in the "<b>CREATE TABLE</b>" Command.

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
# MAGIC * The "<b>Change Data Feed</b>" Option must be "<b>Explicitly Enabled</b>" on an "<b>Existing Table</b>" by "<b>Using</b>" the "<b>Table Property</b>" "<b>delta.enableChangeDataFeed = true</b>" in the "<b>ALTER TABLE</b>" Command.

# COMMAND ----------

# DBTITLE 1,"Enable CDC" for the "Already Existing Table", i.e., "retailer_db.tbl_CustomerWithDeltaFile"
# MAGIC %sql
# MAGIC ALTER TABLE retailer_db.tbl_DeltaStudentWithForeachBatch
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Enable" the "Change Data Feed" on "All New Tables"
# MAGIC * The "<b>Change Data Feed</b>" Option must be "<b>Explicitly Enabled</b>" on "<b>All New Tables</b>" by "<b>Using</b>" the "<b>Spark Configuration</b>" "<b>spark.databricks.delta.properties.defaults.enableChangeDataFeed = true</b>" in the "<b>Cluster" Level</b>", or, in "<b>SparkSession</b>".

# COMMAND ----------

# DBTITLE 1,"Enable CDC" for the "Current SparkSession"
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Change Data Storage"?
# MAGIC * "<b>Databricks</b>" "<b>Records</b>" the "<b>Change in Data</b>" for "<b>UPDATE</b>", "<b>DELETE</b>", and "<b>MERGE</b>" Operations in the "<b>_change_data</b>" Folder "<b>Under</b>" the "<b>Delta Table</b>" Directory.
# MAGIC * These "<b>Records</b>" may be "<b>Skipped</b>" when "<b>Databricks</b>" "<b>Detects</b>" that the "<b>Change Data Feed</b>" can be Efficiently "<b>Computed Directly</b>" from the "<b>Transaction Log</b>". In particular, the "<b>Insert-Only</b>" Operations and the "<b>Full Partition Deletes</b>" will "<b>Not Generate</b>" any "<b>Data</b>" in the "<b>_change_data</b>" Directory.
# MAGIC * The "<b>Files</b>" in the "<b>_change_data</b>" Folder "<b>Follow</b>" the "<b>Retention Policy</b>" of the "<b>Table</b>". Therefore, if the "<b>VACUUM</b>" Command is "<b>Run</b>" on the "<b>Table</b>", the "<b>Change Data Feed</b>" Data is also "<b>Deleted</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # Change Data Event Schema
# MAGIC * In "<b>Addition</b>" to the "<b>Data Columns</b>", the "<b>Change Data</b>" "<b>Contains</b>" the "<b>Metadata Columns</b>" that "<b>Identify</b>" the "<b>Type</b>" of the "<b>Change Event</b>".
# MAGIC * A. <b>_change_type</b>: This Column is of Data Type "<b>String</b>". The "<b>Values</b>" can be - "<b>insert</b>", "<b>update_preimage</b>", "<b>update_postimage</b>", or, "<b>delete</b>".
# MAGIC * "<b>preimage</b>" is the "<b>Value</b>" that is "<b>Before</b>" the "<b>Update</b>".
# MAGIC * "<b>postimage</b>" is the "<b>Value</b>" that is "<b>After</b>" the "<b>Update</b>".
# MAGIC * B. <b>_commit_version</b>: This Column is of Data Type "<b>Long</b>". The "<b>Value</b>" is the "<b>Delta Log</b>", or, the "<b>Table Version</b>" "<b>Containing</b>" the "<b>Change</b>".
# MAGIC * C. <b>_commit_timestamp</b>: This Column is of Data Type "<b>Timestamp</b>". The "<b>Value</b>" is the "<b>Timestamp</b>" that is "<b>Associated</b>" when the "<b>Commit</b>" was "<b>Created</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Read" the "Changes" in "Batch Queries"
# MAGIC * It is possible to "<b>Provide</b>" either the "<b>Version</b>" or the "<b>Timestamp</b>" for the "<b>Start</b>" and "<b>End</b>".
# MAGIC * The "<b>Start</b>" and "<b>End</b>" "<b>Versions</b>" and the "<b>Timestamps</b>" are "<b>Inclusive</b>" in the "<b>Queries</b>".
# MAGIC * To "<b>Read</b>" the "<b>Changes</b>" from a Particular "<b>Start Version</b>" to the "<b>Latest Version</b>" of the "<b>Table</b>", "<b>Specify</b>" Only the "<b>Starting Version</b>" or "<b>Starting Timestamp</b>".
# MAGIC * The "<b>Version</b>" must be "<b>Specified</b>" as an "<b>Integer</b>" and the "<b>Timestamps</b>" must be "<b>Specified</b>" as a "<b>String</b>" in the Format "<b>yyyy-MM-dd[ HH:mm:ss[.SSS]]</b>".
# MAGIC * If a "<b>Version</b>", which is "<b>Lower</b>", or, a "<b>Timestamp</b>", which is "<b>Older</b>" than "<b>One</b>" that has "<b>Recorded</b>" the "<b>Change Events</b>" is "<b>Provided</b>", i.e., "<b>When</b>" the "<b>Change Data Feed</b>" was "<b>Enabled</b>", an "<b>Error</b>" is "<b>Thrown</b>" "<b>Indicating</b>" that the "<b>Change Data Feed</b>" was "<b>Not Enabled</b>".

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
# MAGIC * To "<b>Get</b>" the "<b>Change Data</b>" "<b>While Reading</b>" the "<b>Delta Table</b>", the Option "<b>readChangeFeed</b>" "<b>Must</b>" be "<b>Set</b>" to "<b>True</b>".
# MAGIC * The "<b>startingVersion</b>" or "<b>startingTimestamp</b>" are "<b>Optional</b>" and "<b>If Not Provided</b>", the "<b>Stream</b>" "<b>Returns</b>" the "<b>Latest Snapshot</b>" of the "<b>Delta Table</b>" at the "<b>Time</b>" of "<b>Streaming</b>" as an "<b>INSERT</b>" and "<b>Future Changes</b>" as "<b>Change Data</b>".
# MAGIC * "<b>Options</b>" like the "<b>Rate Limits</b>" ("<b>maxFilesPerTrigger</b>", "<b>maxBytesPerTrigger</b>") and "<b>excludeRegex</b>" are "<b>Also Supported</b>" "<b>When Reading</b>" the "<b>Change Data</b>".
# MAGIC * "<b>Rate Limits</b>" can be "<b>Atomic</b>" for "<b>Versions</b>" "<b>Other Than</b>" the "<b>Starting Snapshot Version</b>". That is, the "<b>Entire Commit Version</b>" will be "<b>Rate Limited</b>" or the "<b>Entire Commit</b>" will be "<b>Returned</b>".

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
# MAGIC * By Default, if a "<b>User</b>" "<b>Passes</b>" in a "<b>Version</b>" or "<b>Timestamp</b>" "<b>Exceeding</b>" the "<b>Last Commit</b>" on a "<b>Delta Table</b>", the "<b>Errors</b>" "<b>VersionNotFoundException</b>", or, "<b>timestampGreaterThanLatestCommit</b>" will be "<b>Thrown</b>".
# MAGIC * "<b>CDF</b>" can "<b>Handle</b>" the "<b>Out of Range</b>" "<b>Version Case</b>", if the "<b>User</b>" "<b>Sets</b>" the "<b>Configuration</b>" "<b>spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled</b>" to "<b>True</b>".

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
# MAGIC * If a "<b>Start Version</b>" is "<b>Provided</b>" which is "<b>Greater Than</b>" the "<b>Last Commit</b>" on a "<b>Delta Table</b>", or, a "<b>Start Timestamp</b>" is "<b>Provided</b>" that is "<b>Newer Than</b>" the "<b>Last Commit</b>" on a "<b>Delta Table</b>", then "<b>When</b>" the "<b>Configuration</b>" "<b>spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled</b>" is "<b>Set</b>" to "<b>True</b>", still the "<b>Errors</b>" "<b>VersionNotFoundException</b>", or, "<b>timestampGreaterThanLatestCommit</b>" will be "<b>Thrown</b>".

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
# MAGIC * If an "<b>End Version</b>" is "<b>Provided</b>" which is "<b>Greater Than</b>" the "<b>Last Commit</b>" on a "<b>Delta Table</b>", or, an "<b>End Timestamp</b>" is "<b>Provided</b>" that is "<b>Newer Than</b>" the "<b>Last Commit</b>" on a "<b>Delta Table</b>", then "<b>When</b>" the "<b>Configuration</b>" "<b>spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled</b>" is "<b>Set</b>" to "<b>True</b>" in "<b>Batch Read</b>" Mode, "<b>All</b>" the "<b>Changes</b>" "<b>Between</b>" the "<b>Start Version</b>" and the "<b>Last Commit</b>" are to be "<b>Returned</b>".

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
