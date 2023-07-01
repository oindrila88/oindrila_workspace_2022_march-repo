# Databricks notebook source
# MAGIC %md
# MAGIC # How Schema is Inferred Using AutoLoader in Databricks
# MAGIC * Topic: How to "Infer" the "Schema" of the "Incoming Streaming Data" Using "Auto Loader"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to the "Schema Inference" in "Auto Loader"
# MAGIC * "Auto Loader" can "Automatically Detect" the "Introduction" of "New Columns" to the "Data" and "Restart" so that the "Developers" "Don’t Have" to "Manage" the "Tracking" and "Handling" of the "Schema Changes".
# MAGIC * "Auto Loader" can also "Rescue" the Data that was "Unexpected" (for example, of "Differing Data Types") in a "JSON Blob Column", that the "Developers" can "Choose" to "Access Later" Using the "Semi-Structured Data Access APIs".
# MAGIC * The following "File Formats" are "Supported" for "Schema Inference" -
# MAGIC * A. JSON
# MAGIC * B. CSV
# MAGIC * C. Avro
# MAGIC * D. Parquet

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Auto Loader" Manages "Schema Inference"
# MAGIC * To "Infer" the "Schema", "Auto Loader" "Samples" the "First 50 GB" or "1000 Files" that it "Discovers", whichever "Limit" is "Crossed First".
# MAGIC * To "Avoid" "Incurring" this "Inference Cost" at "Every Stream Start Up", and to be "Able" to "Provide" a "Stable Schema" Across the "Stream Restarts", the Option "<b>cloudFiles.schemaLocation</b>" Must be "Set". "Auto Loader" "Creates" a "Hidden Directory", i.e., "_schemas" at the "Location" Provided by the Option "<b>cloudFiles.schemaLocation</b>" to "Track" the "Schema Changes" to the "Input Data" Over Time.
# MAGIC * If the "Stream" "Contains" a "Single" "cloudFiles Source" to "Ingest" the Data, it is better to Provide the "Checkpoint Location" as the Option "<b>cloudFiles.schemaLocation</b>". Otherwise, a "Unique Directory" should be Provided for this Option.
# MAGIC * If the "Input Data" "Returns" an "Unexpected Schema" for the "Stream", it is best to "Check" that If the "Schema Location" is "Being Used" by "Only" a "Single Auto Loader Source" or "Not".
# MAGIC * It is possible to "Change" the Default "Size" of the "Sample", i.e., "50GB", that's Used by "Auto Loader" to "Infer" the "Schema" Using the following "Spark Configuration" -
# MAGIC <br><b>spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numBytes", "10gb")</b>. A "Byte String", like "10gb" can be Provided.
# MAGIC * It is possible to "Change" the Default "Number" of the "Sample Files", i.e., "1000 Files", that's Used by "Auto Loader" to "Infer" the "Schema" Using the following "Spark Configuration" -
# MAGIC <br><b>spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", 6)</b>. An "Integer" can be Provided.

# COMMAND ----------

# DBTITLE 1,"Create" a Delta Table "autoloader_customers" Under the Database "training"
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS training.autoloader_customers;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS training.autoloader_customers
# MAGIC (
# MAGIC     Customer_Id STRING,
# MAGIC     First_Name STRING,
# MAGIC     Last_Name STRING,
# MAGIC     City STRING,
# MAGIC     Country STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/with-aad-app/databricks-training-folder/day-4/target-delta-table/autoloader_customers';

# COMMAND ----------

# DBTITLE 1,"Find" the "Path" of the Delta Table "training.autoloader_customers"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED training.autoloader_customers;

# COMMAND ----------

# DBTITLE 1,Check the Data Present in the Delta Table "autoloader_customers"
# MAGIC %sql
# MAGIC SELECT * FROM training.autoloader_customers;

# COMMAND ----------

# DBTITLE 1,Check for the "History" of the Delta Table "training.autoloader_customers"
# MAGIC %sql
# MAGIC DESCRIBE HISTORY training.autoloader_customers;

# COMMAND ----------

# DBTITLE 1,"Change" the Default "Size" of the "Sample" to "1GB"
spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numBytes", "1gb")

# COMMAND ----------

# DBTITLE 1,"Change" the Default "Number" of the "Sample Files" to "1 File"
spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", 1)

# COMMAND ----------

# DBTITLE 1,"Read" the Data from the "Continuous Arriving CSV Files" from a "Directory" in "ADLS Gen2"
from pyspark.sql.types import *

sourceFileLocation = '/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-schema-inference-csv-files'
sourceFileFormat = 'CSV'
schemaLocationPath = '/mnt/with-aad-app/databricks-training-folder/day-4/target-delta-table/autoloader_customers/checkpoint-and-schema-location'

# Read the "Continuous Arriving CSV Files" from the Mounted Path "/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-schema-inference-csv-files" Using "Auto Loader".
df_ReadCustomersCsvFiles = spark.readStream.format("cloudFiles")\
                                           .option("cloudFiles.format", sourceFileFormat)\
                                           .option("header", "true")\
                                           .option("cloudFiles.schemaLocation", schemaLocationPath)\
                                           .load(sourceFileLocation)

# COMMAND ----------

# DBTITLE 1,Display the "Data" of the "Streaming DataFrame"
display(df_ReadCustomersCsvFiles)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Schema Inference" of the "Text-Based Cloud Files" Using "Auto Loader"
# MAGIC * By default, "Auto Loader" "Infers" the "Columns" in "Text-Based File Formats" like "CSV" and "JSON" as "String" Columns.
# MAGIC * In "JSON Datasets", the "Nested Columns" are also "Inferred" as "String" Columns.
# MAGIC * Since "JSON" and "CSV" Data is "Self-Describing" and can "Support" Many "Data Types", "Inferring" the Data as "String" can "Help Avoid" the "Schema Evolution" Issues, such as "Numeric Type Mismatches" ("Integers", "Longs", "Floats").
# MAGIC * It is also possible to "Retain" the "Original Spark Schema Inference Behavior", by "Setting" the Option "<b>cloudFiles.inferColumnTypes</b>"" to "<b>true</b>".

# COMMAND ----------

# DBTITLE 1,"Read" the Data from the "Continuous Arriving CSV Files" from a "Directory" in "ADLS Gen2"
from pyspark.sql.types import *

sourceFileLocation = '/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-schema-inference-csv-files'
sourceFileFormat = 'CSV'
schemaLocationPath = '/mnt/with-aad-app/databricks-training-folder/day-4/target-delta-table/autoloader_customers/checkpoint-and-schema-location'

# Read the "Continuous Arriving CSV Files" from the Mounted Path "/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-schema-inference-csv-files" Using "Auto Loader".
df_ReadCustomersCsvFiles = spark.readStream.format("cloudFiles")\
                                           .option("cloudFiles.format", sourceFileFormat)\
                                           .option("header", "true")\
                                           .option("cloudFiles.schemaLocation", schemaLocationPath)\
                                           .option("cloudFiles.inferColumnTypes", "true")\
                                           .load(sourceFileLocation)

# COMMAND ----------

# DBTITLE 1,Display the "Data" of the "Streaming DataFrame"
display(df_ReadCustomersCsvFiles)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Schema Inference" of the "Partitioned Columns" of the "Text-Based Cloud Files" Using "Auto Loader"
# MAGIC * "Text File" Format, as well as "Binary File" ("binaryFile") Format, have "Fixed Data Schemas", but also "Support" the "Partition Column Inference". The "Partition Columns" are "Inferred" at "Each Stream Restart" Unless the Option "<b>cloudFiles.schemaLocation</b>" is "Specified".
# MAGIC * To "Avoid" Any "Potential Errors" or "Information Loss", Databricks "Recommends" "Setting" the Option "<b>cloudFiles.schemaLocation</b>" or the Option "<b>cloudFiles.partitionColumns</b>" for the "Text File" or "Binary File" Formats as the Option "<b>cloudFiles.schemaLocation</b>" is "Not a Required Option" for these "File Formats".
# MAGIC * "Auto Loader" "Attempts" to "Infer" the "Partition Columns" from the "Underlying Directory Structure" of the Data, if the Data is "Laid Out" in "Hive Style Partitioning".
# MAGIC * Example - a "File Path", such as "base_path/event=click/date=2021-04-01/f0.json" would "Result" in the "Inference" of "date" and "event" as the "Partition Columns". The "Data Types" for these "Columns" will be "Strings" unless the Option "<b>cloudFiles.inferColumnTypes</b>" is "Set" to "<b>true</b>".
# MAGIC * If the "Underlying Directory Structure" "Contains" some "Conflicting Hive Partitions" or "Doesn’t Contain" Any "Hive Style Partitioning", then the "Partition Columns" will be "Ignored".
# MAGIC * It is possible to Provide the Option "<b>cloudFiles.partitionColumns</b>" as a "<b>Comma-Separated List Column Names</b>" to Always "Try" and "Parse" the "Given Columns" from the "File Path", if these "Columns" "Exist" as "Key=Value Pairs" in the "Directory Structure".

# COMMAND ----------

# MAGIC %md
# MAGIC # Providing "Schema Hints" Using "Auto Loader"
# MAGIC * The "Data Types" that are "Inferred" may "Not Always" be "Exactly" What is "Expected". By Using the "Schema Hints", it is possible to "Superimpose" the "Information" that is "Already Known" and "Expected" on an "Inferred Schema".
# MAGIC * By default, "Apache Spark" has a "Standard Approach" for "Inferring" the "Data Type" of the "Columns". Example - "Apache Spark" "Infers" the "Nested JSON" as "Structs" and "Integers" as "Longs".
# MAGIC * In Contrast, "Auto Loader" "Considers" "All Columns" as "Strings". When it is "Known" that a "Column" is of a "Specific" "Data Type", or if it is desired to "Choose" an Even More "General Data Type" (Example - a "Double" Instead of an "Integer"), it is possible to Provide an "Arbitrary Number" of "Hints" for the "Data Type" of the "Columns" as - "<b>.option("cloudFiles.schemaHints", "age int, salary double")</b>".
# MAGIC * If a "Column" is "Not Present" at the "Start" of the "Stream", it is also possible to "Add" that "Column" to the "Inferred Schema" Using the "Schema Hints".
# MAGIC * "Schema Hints" are Used "Only If" "No Schema" is Provided to a "Auto Loader".
# MAGIC * It is possible to Use the "Schema Hints" "Whether" the Option "<b>cloudFiles.inferColumnTypes</b>" is "<b>Enabled</b>" or "<b>Disabled</b>".

# COMMAND ----------

# DBTITLE 1,"Read" the Data from the "Continuous Arriving CSV Files" from a "Directory" in "ADLS Gen2"
from pyspark.sql.types import *

sourceFileLocation = '/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-schema-inference-csv-files'
sourceFileFormat = 'CSV'
schemaLocationPath = '/mnt/with-aad-app/databricks-training-folder/day-4/target-delta-table/autoloader_customers/checkpoint-and-schema-location'
customerSchemaHint = "Customer_Id string, Full_Name string, Date_Of_Birth date, Age int"
rescuedDataColumnName = '_rescued_data_column'

# Read the "Continuous Arriving CSV Files" from the Mounted Path "/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-schema-inference-csv-files" Using "Auto Loader".
df_ReadCustomersCsvFiles = spark.readStream.format("cloudFiles")\
                                           .option("cloudFiles.format", sourceFileFormat)\
                                           .option("cloudFiles.rescuedDataColumn", rescuedDataColumnName)\
                                           .option("header", "true")\
                                           .option("dateFormat", "dd-MM-yyyy")\
                                           .option("cloudFiles.schemaLocation", schemaLocationPath)\
                                           .option("cloudFiles.inferColumnTypes", "true")\
                                           .option("cloudFiles.schemaHints", customerSchemaHint)\
                                           .load(sourceFileLocation)

# COMMAND ----------

# DBTITLE 1,Display the "Data" of the "Streaming DataFrame"
display(df_ReadCustomersCsvFiles)

# COMMAND ----------

# DBTITLE 1,"Apply" the "Transformation" on the "Streaming DataFrame"
from pyspark.sql.functions import *

df_ReadCustomersCsvFiles = df_ReadCustomersCsvFiles.select(
                                                              col("Customer_Id"),
                                                              col("First_Name"),
                                                              col("Last_Name"),
                                                              col("Date_Of_Birth"),
                                                              col("City"),
                                                              col("Country"),
                                                              concat("First_Name", "Last_Name").alias("Full_Name"),
                                                              (months_between(current_date(), col("Date_Of_Birth"))/12).cast("int").alias("Age")
                                                          )

# COMMAND ----------

# DBTITLE 1,After "Transformation", Display the "Data" of the "Streaming DataFrame"
display(df_ReadCustomersCsvFiles)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rescued Data Column
# MAGIC * The "Rescued Data Column" "Ensures" that the Users "Never Lose Out" or "Never Miss Out" on "Data" "During ETL".
# MAGIC * The "Rescued Data Column" "Contains" Any "Data" that "Wasn't Parsed", because of the following reasons -
# MAGIC * A. The "Column" was "Missing" from the "Given Schema".
# MAGIC * B. There might have been a "Type Mismatch" between the "Incoming Stream" and "Schema".
# MAGIC * C. The "Casing" of the "Column" in the "Record" or "File" "Didn’t Match" with that in the "Schema".
# MAGIC * The "Rescued Data Column" is "Returned" as a "JSON Blob" "Containing" the "Columns" that were "Rescued", and the "Source File Path" of the "Record".
# MAGIC * To "Remove" the "Source File Path" from the "Rescued Data Column", "Set" the following Option in the "Spark Configuration" -
# MAGIC * "<b>spark.conf.set("spark.databricks.sql.rescuedDataColumn.filePath.enabled", "false")</b>".
# MAGIC * The "Rescued Data Column" is "Part" of the "Schema" that is "Returned" by the "Auto Loader" as "<b>_rescued_data</b>" By Default When the "schema" is being "Inferred".
# MAGIC * It is possible to "Rename" the "Column" "<b>_rescued_data</b>" or "Include" it in "Cases" where a "Schema" is "Provided" by "Setting" the Option "<b>rescuedDataColumn</b>".
# MAGIC * Example - "<b>spark.readStream.format("cloudFiles").option("cloudFiles.rescuedDataColumn", "_rescued_data_column")</b>".
# MAGIC * "Since" the "Default Value" of the Option "<b>cloudFiles.inferColumnTypes</b>" is "<b>false</b>", and the "Default Value" of the Option "<b>cloudFiles.schemaEvolutionMode</b>" is "<b>addNewColumns</b>" When the "Schema" is Being "Inferred", the "Rescued Data Column" "Captures" Only the "Columns" that "Have" a "Different Case" Than that in the "Schema".

# COMMAND ----------

# DBTITLE 1,"Read" the Data from the "Continuous Arriving CSV Files" from a "Directory" in "ADLS Gen2"
from pyspark.sql.types import *

sourceFileLocation = '/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-schema-inference-csv-files'
sourceFileFormat = 'CSV'
schemaLocationPath = '/mnt/with-aad-app/databricks-training-folder/day-4/target-delta-table/autoloader_customers/checkpoint-and-schema-location'
customerSchemaHint = "Customer_Id string, Full_Name string, Date_Of_Birth date, Age int"
rescuedDataColumnName = '_rescued_data_column'

# Read the "Continuous Arriving CSV Files" from the Mounted Path "/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-schema-inference-csv-files" Using "Auto Loader".
df_ReadCustomersCsvFiles = spark.readStream.format("cloudFiles")\
                                           .option("cloudFiles.format", sourceFileFormat)\
                                           .option("cloudFiles.rescuedDataColumn", rescuedDataColumnName)\
                                           .option("header", "true")\
                                           .option("cloudFiles.schemaLocation", schemaLocationPath)\
                                           .option("cloudFiles.inferColumnTypes", "true")\
                                           .option("cloudFiles.schemaHints", customerSchemaHint)\
                                           .load(sourceFileLocation)

# COMMAND ----------

# DBTITLE 1,Display the "Data" of the "Streaming DataFrame"
display(df_ReadCustomersCsvFiles)

# COMMAND ----------

# DBTITLE 1,"Remove" the "File Path" from the "Rescued Data Column"
from pyspark.sql.types import *

# "Remove" the "Source File Path" from the "Rescued Data Column".
spark.conf.set("spark.databricks.sql.rescuedDataColumn.filePath.enabled", "false")

sourceFileLocation = '/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-schema-inference-csv-files'
sourceFileFormat = 'CSV'
schemaLocationPath = '/mnt/with-aad-app/databricks-training-folder/day-4/target-delta-table/autoloader_customers/checkpoint-and-schema-location'
customerSchemaHint = "Customer_Id string, Full_Name string, Date_Of_Birth date, Age int"
rescuedDataColumnName = '_rescued_data_column'

# Read the "Continuous Arriving CSV Files" from the Mounted Path "/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-schema-inference-csv-files" Using "Auto Loader".
df_ReadCustomersCsvFiles = spark.readStream.format("cloudFiles")\
                                           .option("cloudFiles.format", sourceFileFormat)\
                                           .option("cloudFiles.rescuedDataColumn", rescuedDataColumnName)\
                                           .option("header", "true")\
                                           .option("cloudFiles.schemaLocation", schemaLocationPath)\
                                           .option("cloudFiles.inferColumnTypes", "true")\
                                           .option("cloudFiles.schemaHints", customerSchemaHint)\
                                           .load(sourceFileLocation)

# COMMAND ----------

# DBTITLE 1,Display the "Data" of the "Streaming DataFrame"
display(df_ReadCustomersCsvFiles)
