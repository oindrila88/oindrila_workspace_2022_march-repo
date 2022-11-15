# Databricks notebook source
# MAGIC %md
# MAGIC # How to Use "Change Data Feed" ("CDF") in "MERGE" Statement in Databricks
# MAGIC * Topic: Introduction to Using "Change Data Feed" ("CDF") in "MERGE" Statement
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Using "Change Data Feed" (CDF) "Row Data" in a "MERGE" Statement
# MAGIC * "Aggregate MERGE" Statements, like the "MERGE INTO" the "Silver", or, "Gold" Delta Table, can be "Complex" by nature, but the "Change Data Feed" (CDF) Feature makes the "Coding" of these "Statements" "Simpler" and "More Efficient".
# MAGIC * <img src = '/files/tables/images/merge_cdc.jpg'>
# MAGIC * "CDF" makes it Simple to "Derive" which "Rows" have "Changed", as it Only "Performs" the "Needed Aggregation" on the "Data" that "Has Changed", or, is "New" using the "table_changes" Operation.

# COMMAND ----------

# DBTITLE 1,"Create" an "Audit Table" to "Store" the "Version" of the "Silver" Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retailer_db.auto_mapper
# MAGIC (
# MAGIC     DatabaseName STRING,
# MAGIC     TableName STRING,
# MAGIC     VersionNumber INT
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,"Create" a "Function" to "Update" the Value of the Column "VersionNumber" for "Each Silver Table"
def updateVersionInAutoMapper(databaseName, tableName, endVersion):
    latestVersion = int(endVersion) + 1
    spark.sql(f"UPDATE retailer_db.auto_mapper SET VersionNumber = {latestVersion} WHERE DatabaseName = '{databaseName}' AND TableName = '{tableName}'")

# COMMAND ----------

# DBTITLE 1,"Create" a "New Silver Table", i.e., "retailer_db.slvr_StudentWithCdcEnabled" With "CDC Enabled"
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retailer_db.slvr_StudentWithCdcEnabled
# MAGIC (
# MAGIC     FirstName STRING,
# MAGIC     LastName STRING,
# MAGIC     Subject STRING,
# MAGIC     Marks INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/with-aad-app/databricks-training-folder/day-3/target-delta-table/slvr_StudentWithCdcEnabled/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# DBTITLE 1,"Insert" the "Entry" into the "Audit Table" for the "Silver Table", i.e., "retailer_db.slvr_StudentWithCdcEnabled"
# MAGIC %sql
# MAGIC INSERT INTO retailer_db.auto_mapper VALUES('retailer_db', 'slvr_StudentWithCdcEnabled', 0);

# COMMAND ----------

# DBTITLE 1,"Read" the Data from the "Continuous Arriving CSV Files" from a "Directory" in "ADLS Gen2"
from pyspark.sql.types import *

sourceFileLocation = '/mnt/with-aad-app/databricks-training-folder/day-3/autoloader-csv-files/'
sourceFileFormat = 'CSV'
bronzeDeltaTableName = 'retailer_db.brnz_StudentWithCdcEnabled'
bronzeDeltaTablePath = '/mnt/with-aad-app/databricks-training-folder/day-3/bronze-delta-table/brnz_StudentWithCdcEnabled/'
checkpointDirectoryPath = '/mnt/with-aad-app/databricks-training-folder/day-3/bronze-delta-table/autoloader-checkpoint-directory/brnz_StudentWithCdcEnabled/'
appId = 'brnz_StudentWithCdcEnabled-example'

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
                       .option("path", bronzeDeltaTablePath)\
                       .saveAsTable(bronzeDeltaTableName)
    except Exception as e:
        raise e

# COMMAND ----------

# DBTITLE 1,"Store" the Data from the "Streaming DataFrame" to the "Bronze Delta Table"
df_ReadStudentCsvFiles.writeStream\
                      .format("delta")\
                      .outputMode("append")\
                      .option("checkpointLocation", checkpointDirectoryPath)\
                      .queryName(f"Running Auto Loader for Table '{bronzeDeltaTableName}'")\
                      .trigger(availableNow = True)\
                      .foreachBatch(processData)\
                      .start()

# COMMAND ----------

# DBTITLE 1,Display the Data of the Bronze Delta Table "retailer_db.brnz_StudentWithCdcEnabled"
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.brnz_StudentWithCdcEnabled;

# COMMAND ----------

# DBTITLE 1,"Create" a "View" With the "Changed" or "Latest" Data from the "Bronze Table"
bronzeDatabaseName = 'retailer_db'
bronzeTableName = 'brnz_StudentWithCdcEnabled'
silverDatabaseName = 'retailer_db'
silverTableName = 'brnz_StudentWithCdcEnabled'
latestOrChangedDataViewName = 'vw_bronzeTableChangedOrLatestData'

startVersion = spark.sql(f"SELECT VersionNumber FROM retailer_db.auto_mapper WHERE DatabaseName = '{silverDatabaseName}' AND TableName = '{silverTableName}'").collect()[0][0]
endVersion = spark.sql("SELECT MAX(_commit_version) FROM table_changes('retailer_db.brnz_StudentWithCdcEnabled', 0)").collect()[0][0]

if int(startVersion) <= int(endVersion):
    df_bronzeTableChangedOrLatestData = spark.read.format("delta")\
                                                  .option("readChangeFeed", "true")\
                                                  .option("startingVersion", startVersion)\
                                                  .option("endingVersion", endVersion)\
                                                  .table(bronzeDatabaseName + "." + bronzeTableName)
    
    df_bronzeTableChangedOrLatestData.dropDuplicates()
    
    df_bronzeTableChangedOrLatestData.createOrReplaceTempView(latestOrChangedDataViewName)

# COMMAND ----------

# MAGIC %md
# MAGIC * you can use the changed data from the silver table to aggregate only the data on the rows that need to be updated or inserted into the gold table. To do this, use INNER JOIN on the table_changes(‘table_name’,’version’)

# COMMAND ----------

# DBTITLE 1,"Merge" the "Changed", or, the "Latest" Data from "Bronze" Table to "Silver" Table
spark.sql(f"""
MERGE INTO {silverDatabaseName}.{silverTableName} Target
USING {latestOrChangedDataViewName} Source
ON Target.FirstName = Source.FirstName AND Target.LastName = Source.LastName AND Target.Marks = Source.Marks
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED
    THEN INSERT *
""")
