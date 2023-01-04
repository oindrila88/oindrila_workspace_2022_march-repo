# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to "Incremental ETL" Approach in Databricks
# MAGIC * Topic: How to Ingest the "Data" into an "Empty Delta Table" Using "Incremental ETL"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "COPY INTO" SQL Command
# MAGIC * The "COPY INTO" SQL Command lets the Developers "Load" the "Data" from a "File Location" into a "Delta Table".
# MAGIC * This is a "Re-Triable" and "Idempotent" Operation. That means the "Files" in the "Source Location" that have "Already" been "Loaded" are "Skipped".

# COMMAND ----------

# MAGIC %md
# MAGIC # When "COPY INTO" SQL Command is Used?
# MAGIC * The "COPY INTO" SQL Command should be used when -
# MAGIC * A. When the "Source File Location" Contains "Files" in the Order of "Thousands" or "Fewer".
# MAGIC * B. When the "Schema" of the Data is "Not Expected" to "Evolve Frequently".
# MAGIC * C. When the "Subsets" of "Previously Uploaded Files" are "Planned" to be "Loaded".

# COMMAND ----------

# DBTITLE 1,"Create" a Database "training"
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS training;

# COMMAND ----------

# DBTITLE 1,"Create" a Delta Table "students_copy_into" Under the Database "training"
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS training.students_copy_into;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS training.students_copy_into
# MAGIC (
# MAGIC     FirstName STRING,
# MAGIC     MiddleName STRING,
# MAGIC     LastName STRING,
# MAGIC     Subject STRING,
# MAGIC     Marks INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,"Create" a Delta Table "students_copy_into_with_metadata" Under the Database "training"
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS training.students_copy_into_with_metadata;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS training.students_copy_into_with_metadata
# MAGIC (
# MAGIC     FirstName STRING,
# MAGIC     MiddleName STRING,
# MAGIC     LastName STRING,
# MAGIC     Subject STRING,
# MAGIC     Marks INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,"Create" a Delta Table "students_copy_into_with_inner_metadata" Under the Database "training"
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS training.students_copy_into_with_inner_metadata;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS training.students_copy_into_with_inner_metadata
# MAGIC (
# MAGIC     FirstName STRING,
# MAGIC     MiddleName STRING,
# MAGIC     LastName STRING,
# MAGIC     Subject STRING,
# MAGIC     Marks INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Check the Data Present in the Delta Table "students_copy_into"
# MAGIC %sql
# MAGIC SELECT * FROM training.students_copy_into;

# COMMAND ----------

# DBTITLE 1,Check the Data Present in the Delta Table "students_copy_into_with_metadata"
# MAGIC %sql
# MAGIC SELECT * FROM training.students_copy_into_with_metadata;

# COMMAND ----------

# DBTITLE 1,Check the Data Present in the Delta Table "students_copy_into_with_inner_metadata"
# MAGIC %sql
# MAGIC SELECT * FROM training.students_copy_into_with_inner_metadata;

# COMMAND ----------

# DBTITLE 1,"Load" the Data into the Delta Table Using "COPY INTO" SQL Command
tableName = 'training.students_copy_into'
sourceFileLocation = 'dbfs:/FileStore/tables/training/csv-files'
sourceFileFormat = 'CSV'

spark.sql(f"""COPY INTO {tableName}
              FROM '{sourceFileLocation}'
              FILEFORMAT = {sourceFileFormat}
              FORMAT_OPTIONS('HEADER' = 'TRUE', 'inferSchema' = 'TRUE')
         """)

# COMMAND ----------

# DBTITLE 1,"Load" the Data into the Delta Table Using "COPY INTO" SQL Command With Metadata Columns
tableName = 'training.students_copy_into_with_metadata'
sourceFileLocation = 'dbfs:/FileStore/tables/training/csv-files'
sourceFileFormat = 'CSV'

spark.sql(f"""COPY INTO {tableName}
              FROM
              (
                  SELECT *,
                  _metadata
                  FROM '{sourceFileLocation}'
              )
              FILEFORMAT = {sourceFileFormat}
              FORMAT_OPTIONS('MERGESCHEMA' = 'TRUE', 'HEADER' = 'TRUE', 'inferSchema' = 'TRUE')
              COPY_OPTIONS('MERGESCHEMA' = 'TRUE')
         """)

# COMMAND ----------

# DBTITLE 1,"Load" the Data into the Delta Table Using "COPY INTO" SQL Command With Each of the Supported Inner Metadata Columns
tableName = 'training.students_copy_into_with_inner_metadata'
sourceFileLocation = 'dbfs:/FileStore/tables/training/csv-files'
sourceFileFormat = 'CSV'

spark.sql(f"""COPY INTO {tableName}
              FROM
              (
                  SELECT *,
                  _metadata.file_path,
                  _metadata.file_name,
                  _metadata.file_size,
                  _metadata.file_modification_time
                  FROM '{sourceFileLocation}'
              )
              FILEFORMAT = {sourceFileFormat}
              FORMAT_OPTIONS('MERGESCHEMA' = 'TRUE', 'HEADER' = 'TRUE', 'inferSchema' = 'TRUE')
              COPY_OPTIONS('MERGESCHEMA' = 'TRUE')
         """)

# COMMAND ----------

# DBTITLE 1,Drop the Table "training.students_copy_into"
# MAGIC %sql
# MAGIC DROP TABLE training.students_copy_into;

# COMMAND ----------

# DBTITLE 1,Drop the Table "training.students_copy_into_with_metadata"
# MAGIC %sql
# MAGIC DROP TABLE training.students_copy_into_with_metadata;

# COMMAND ----------

# DBTITLE 1,Drop the Table "training.students_copy_into_with_inner_metadata"
# MAGIC %sql
# MAGIC DROP TABLE training.students_copy_into_with_inner_metadata;

# COMMAND ----------

# MAGIC %md
# MAGIC # Auto Loader
# MAGIC * "Auto Loader" Incrementally and Efficiently Processes the "New Data Files" as those Arrive in the "Cloud Storage" without any Additional Setup.
# MAGIC * "Auto Loader" Provides a "New Structured Streaming Source" called "cloudFiles".
# MAGIC * Given an "Input Directory Path" on the "Cloud File Storage", the "cloudFiles" Source "Automatically" Processes the "New Files" as those Arrive, with the Option of Also Processing the "Existing Files" in that "Directory".

# COMMAND ----------

# MAGIC %md
# MAGIC # When "Auto Loader" is Used?
# MAGIC * The "Auto Loader" should be used when -
# MAGIC * A. When the "Source File Location" Contains the "Files" in the Order of "Millions" or "Higher". "Auto Loader" can Discover the "Files" more Efficiently than the "COPY INTO" SQL Command and can "Split" the "File Processing" into "Multiple Batches".
# MAGIC * B. When the "Schema" of the Data "Evolves" Frequently. "Auto Loader" Provides "Better Support" for "Schema Inference" and "Evolution".
# MAGIC * C. When "No Subsets" of the "Previously Uploaded Files" are "Planned" to be "Loaded". With "Auto Loader", it can be "More Difficult" to "Re-Process" the "Subsets" of "Files". However, it is possible to Use the "COPY INTO" SQL Command to "Re-Load" the "Subsets" of "Files" while an "Auto Loader Stream" is "Simultaneously Running".

# COMMAND ----------

# MAGIC %md
# MAGIC # How to Use Auto Loader?
# MAGIC * To Use "Auto Loader" Perform the following Steps -
# MAGIC * A. Specify "cloudFiles" as the "Format" for the "Data Stream".
# MAGIC * B. Specify the Path of the "Source File Directory" in the "Data Lake", i.e., "Azure Data Lake Storage Gen2" to "Monitor" for "Arival" of "New Files".
# MAGIC * C. Once the "New Files" Arrive, "Auto Loader" Efficiently and Incrementally Loads the Data from the "New Files" into the Specified "Delta Lake Table".

# COMMAND ----------

# DBTITLE 1,"Create" a Delta Table "autoloader_students" Under the Database "training"
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS training.autoloader_students;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS training.autoloader_students
# MAGIC (
# MAGIC     FirstName STRING,
# MAGIC     MiddleName STRING,
# MAGIC     LastName STRING,
# MAGIC     Subject STRING,
# MAGIC     Marks INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/with-aad-app/databricks-training-folder/day-3/target-delta-table/autoloader_students';

# COMMAND ----------

# DBTITLE 1,"Find" the "Path" of the Delta Table "training.autoloader_students"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED training.autoloader_students;

# COMMAND ----------

# DBTITLE 1,Check the Data Present in the Delta Table "training.autoloader_students"
# MAGIC %sql
# MAGIC SELECT * FROM training.autoloader_students;

# COMMAND ----------

# DBTITLE 1,Check for the "History" of the Delta Table "training.autoloader_students"
# MAGIC %sql
# MAGIC DESCRIBE HISTORY training.autoloader_students;

# COMMAND ----------

# DBTITLE 1,"Read" the Data from the "Continuous Arriving CSV Files" from a "Directory" in "ADLS Gen2"
from pyspark.sql.types import *

sourceFileLocation = '/mnt/with-aad-app/databricks-training-folder/day-3/autoloader-csv-files'
sourceFileFormat = 'CSV'

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

# DBTITLE 1,"Read" the Data from the "Continuous Arriving CSV Files" from a "Directory" in "ADLS Gen2" With Metadata Columns
from pyspark.sql.types import *

sourceFileLocation = '/mnt/with-aad-app/databricks-training-folder/day-3/autoloader-csv-files'
sourceFileFormat = 'CSV'

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
                                         .option("mergeSchema", "true")\
                                         .schema(studentsDataSchema)\
                                         .load(sourceFileLocation)\
                                         .select(
                                                    "*",
                                                    "_metadata"
                                                )

display(df_ReadStudentCsvFiles)

# COMMAND ----------

# DBTITLE 1,"Read" the Data from the "Continuous Arriving CSV Files" from a "Directory" in "ADLS Gen2" With Each of the Supported Inner Metadata Columns
from pyspark.sql.types import *

sourceFileLocation = '/mnt/with-aad-app/databricks-training-folder/day-3/autoloader-csv-files'
sourceFileFormat = 'CSV'

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
                                         .option("mergeSchema", "true")\
                                         .schema(studentsDataSchema)\
                                         .load(sourceFileLocation)\
                                         .select(
                                                    "*",
                                                    "_metadata.file_path",
                                                    "_metadata.file_name",
                                                    "_metadata.file_size",
                                                    "_metadata.file_modification_time"
                                                )

display(df_ReadStudentCsvFiles)

# COMMAND ----------

# DBTITLE 1,"Store" the Data from the "Streaming DataFrame" to the "Delta Table"
tableName = 'training.autoloader_students'
targetTablePath = '/mnt/with-aad-app/databricks-training-folder/day-3/target-delta-table/autoloader_students'
checkpointDirectoryPath = '/mnt/with-aad-app/databricks-training-folder/day-3/autoloader-checkpoint-directory'

# "Start" the "Stream".
# "Use" the "Checkpoint Directory Location" to "Keep" a "Record" of All the "Files" that have "Already" been "Uploaded" to the "sourceFileLocation" Path.
# For those "Files" that have been "Uploaded" since the "Last Check", "Write" the Data of the "Newly-Uploaded Files" to the "targetTablePath" Path.
df_ReadStudentCsvFiles.writeStream.format("delta")\
                                  .outputMode("append")\
                                  .option("checkpointLocation", checkpointDirectoryPath)\
                                  .queryName(f"Running Auto Loader for Table '{tableName}'")\
                                  .start(targetTablePath)

# COMMAND ----------

# MAGIC %md
# MAGIC # Use Auto Loader to Run the Batch Workload
# MAGIC * Running "Auto Loader" Continuously is a "No-Brainer" when the Data Arrives in "Every Hour" or "Every Day". In that case, "Auto Loader" can be Run in "Batch Mode" by Specifying the Option "trigger(once = True)" and "Set Up" the "Notebook" to Run as a "Scheduled Job".
# MAGIC * In the "trigger(once = True)" Mode, "Auto Loader" still keeps track of the "New Arriving Files" "Even When" there is "No Active Cluster Running".
# MAGIC * In the "trigger(once = True)" Mode, "Auto Loader" just "Waits" to "Actually Process" the "New Arriving Files" When the "Code" for "Auto Loader" is Run "Manually" or as Part of the "Scheduled Job".

# COMMAND ----------

# DBTITLE 1,Run the "Auto Loader" in "Batch Mode"
tableName = 'training.autoloader_students'
targetTablePath = '/mnt/with-aad-app/databricks-training-folder/day-3/target-delta-table/autoloader_students'
batchModeCheckpointDirectoryPath = '/mnt/with-aad-app/databricks-training-folder/day-3/autoloader-checkpoint-directory'

# "Start" the "Stream".
# "Use" the "Checkpoint Directory Location" to "Keep" a "Record" of All the "Files" that have "Already" been "Uploaded" to the "sourceFileLocation" Path.
# For those "Files" that have been "Uploaded" since the "Last Check", "Write" the Data of the "Newly-Uploaded Files" to the "targetTablePath" Path.
df_ReadStudentCsvFiles.writeStream.format("delta")\
                                  .trigger(once = True)\
                                  .outputMode("append")\
                                  .option("checkpointLocation", batchModeCheckpointDirectoryPath)\
                                  .queryName(f"Running Auto Loader for Table '{tableName}' In Batch Mode")\
                                  .start(targetTablePath)

# COMMAND ----------

# DBTITLE 1,Drop Table "training.autoloader_students"
# MAGIC %sql
# MAGIC DROP TABLE training.autoloader_students;
