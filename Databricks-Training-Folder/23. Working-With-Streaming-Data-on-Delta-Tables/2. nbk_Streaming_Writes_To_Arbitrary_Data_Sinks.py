# Databricks notebook source
# MAGIC %md
# MAGIC # How to Write Arbitrary Streaming Data in Sinks in Databricks
# MAGIC * Topic: How to Perform "Streaming Writes" to "Arbitrary Data Sinks" with "Structured Streaming"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Perform "Streaming Writes" to "Arbitrary Data Sinks" with "Structured Streaming"
# MAGIC * "Structured Streaming APIs" "Provide" "Two Ways" to "Write" the "Output" of a "Streaming Query" to "Data Sources" that "Do Not" have an "Existing Streaming Sink" -
# MAGIC * A. <b>foreachBatch()</b>
# MAGIC * B. <b>foreach()</b>

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to "foreachBatch()"
# MAGIC * "streamingDF.writeStream.foreachBatch(...)" "Allows" to "Specify" a "Function" that is "Executed" on the "Output Data" of "Every Micro-Batch" of the "Streaming Query".
# MAGIC * It takes "Two Parameters" -
# MAGIC * A. The "DataFrame" or "Dataset" that has the "Output Data" of a "Micro-Batch", and,
# MAGIC * B. The "Unique ID" of the "Micro-Batch".
# MAGIC * With "foreachBatch()", it is possible to -
# MAGIC * I. <b>Reuse Existing Batch Data Sources</b>: For many "Storage Systems", there may "Not" be a "Streaming Sink" Available yet, but there may "Already Exist" a "Data Writer" for "Batch Queries". Using "foreachBatch()"", it is possible to "Use" the "Batch Data Writers" on the "Output" of "Each Micro-Batch".
# MAGIC * II. <b>Write to Multiple Locations</b>: If the "Output" of a "Streaming Query" "Needs" to be "Written" to "Multiple Locations", then it is possible to "Simply Write" the "Output DataFrame"/"Output Dataset" "Multiple Times".
# MAGIC * However, "Each Attempt" to "Write" can "Cause" the "Output Data" to be "Recomputed" (including "Possible Re-Reading" of the "Input Data").
# MAGIC * To "Avoid" the "Recomputations", it is required to "Cache" the "Output DataFrame"/"Output Dataset", "Write" it to "Multiple Locations", and then "Uncache" it.
# MAGIC * III. <b>Apply Additional DataFrame Operations</b>: Many "DataFrame" and "Dataset" Operations are "Not Supported" in the "Streaming DataFrames" because "Spark" "Does Not Support" "Generating" the "Incremental Plans" in those cases.
# MAGIC * Using "foreachBatch()" it is possible to "Apply" some of those "Operations" on "Each Micro-Batch Output", e.g., it is possible to "Use" the "foreachBath()" and the "SQL MERGE INTO" Operation to "Write" the "Output" of the "Streaming Aggregations" into a "Delta Table" in "Update" Mode.

# COMMAND ----------

# DBTITLE 1,"Read" the Data from the "Continuous Arriving CSV Files" from a "Directory" in "ADLS Gen2"
from pyspark.sql.types import *

sourceFileLocation = '/mnt/with-aad-app/databricks-training-folder/day-3/autoloader-csv-files/'
sourceFileFormat = 'CSV'
targetDeltaTablePath = '/mnt/with-aad-app/databricks-training-folder/day-3/target-delta-table/foreachBatch-delta-students/'
checkpointDirectoryPath = '/mnt/with-aad-app/databricks-training-folder/day-3/target-delta-table/autoloader-checkpoint-directory/foreachBatch-delta-students/'
targetDeltaTableName = 'retailer_db.tbl_DeltaStudentWithForeachBatch'
appId = 'foreachBatch-delta-students-example'

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

# DBTITLE 1,Display the Data of the Delta Table "retailer_db.tbl_DeltaStudentWithForeachBatch"
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_DeltaStudentWithForeachBatch;

# COMMAND ----------

# MAGIC %md
# MAGIC # Important Features of "foreachBatch()"
# MAGIC * A. "foreachBatch()" "Provides" the "Only" "At-Least-Once Write Guarantees".
# MAGIC * However, it is possible to "Use" the "batchId" that is "Provided" to the "Function" as a "Way" to "De-Duplicate" the "Output" and "Get" an "Exactly-Once Guarantee". In either case, the "Developers" will have to "Reason" about the "End-to-End Semantics".
# MAGIC * B. "foreachBatch()" "Does Not Work" with the "Continuous Processing Mode" as it "Fundamentally Relies" on the "Micro-Batch Execution" of a "Streaming Query".

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to "foreach()"
# MAGIC * If "foreachBatch()"" is "Not an Option", then it is possible to "Express" the "Custom Writer Logic" using "foreach()".
# MAGIC * Specifically, it is possible to "Express" the "Data Writing Logic" by "Dividing" it into "Three Methods" -
# MAGIC * A. "open()"
# MAGIC * B. "process()"
# MAGIC * C. "close()"
