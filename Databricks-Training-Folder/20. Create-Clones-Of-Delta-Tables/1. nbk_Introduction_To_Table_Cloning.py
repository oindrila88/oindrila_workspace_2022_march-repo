# Databricks notebook source
# MAGIC %md
# MAGIC # Create Clones of Delta Tables in Databricks
# MAGIC * Topic: Introduction to "Cloning" of a "Delta Table".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Table Cloning"?
# MAGIC * It is possible to "Create" a "Copy" of an "Existing Delta Table" at a "Specific Version" using the "CLONE" Command.
# MAGIC * "Clones" are the "Replicas" of a "Source Table" at a given "Point in Time".
# MAGIC * "Cloned Tables" have the "Same Metadata" as the "Source Table", i.e., the "Same Schema", "Same Constraints", "Same Column Descriptions", "Same Nullability Information", "Same Statistics", and "Same Partitioning". However, the "Cloned Tables" behave as a "Separate Table" with a "Separate Lineage" or "Separate History".
# MAGIC * Any "Changes" made to the "Cloned Table" only affects the "Cloned Table" and not the "Source Table".
# MAGIC * Any "Changes" that happen to the "Source Table" during or after the "Cloning Process" also do "Not" get "Reflected" in the "Cloned Table" due to "Snapshot Isolation".
# MAGIC * In "Databricks Delta Lake" there are "Two Types" of "Cloned Tables" -
# MAGIC * A. Shallow Cloned Tables
# MAGIC * B. Deep Cloned Tables

# COMMAND ----------

# MAGIC %md
# MAGIC # "CTAS" vs. "Table Cloning"
# MAGIC * "Cloning" a "Delta Table" is "Not" the "Same" as "CREATE TABLE AS SELECT" or "CTAS". A "Cloned Table" "Copies" the "Metadata" of the "Source Table" in addition to the "Data".
# MAGIC * "Cloning" also has "Simpler Syntax". Hence, there is no need to specify "Partitioning", "Format", "Invariants", "Nullability" and so on as these "Information" are taken from the "Source Table".

# COMMAND ----------

# MAGIC %md
# MAGIC # Why "Table Cloning" is Required?
# MAGIC * Creating "Copies" of "Tables" in a "Data Lake" or "Data Warehouse" has several "Practical Uses".
# MAGIC * However, given the "Volume" of "Data" in "Tables" in a "Data Lake" and the "Rate" of its growth, making "Physical Copies" of "Tables" is an "Expensive Operation".
# MAGIC * "Databricks Delta Lake" now makes the process "Simpler" and "Cost-Effective" with the help of "Table Clones".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Shallow Cloned Tables"?
# MAGIC * A "Shallow Cloned Table" (also known as "Zero-Copy") only "Copies" the "Metadata" of the "Source Table" being "Cloned". The "Data Files" of the "Source Table" itself are "Not Copied".
# MAGIC * This type of "Cloning" does "Not Create" "Another Physical Copy" of the "Data" resulting in "Minimal Storage Costs".
# MAGIC * "Shallow Cloned Tables" are "Inexpensive" and can be "Extremely Fast" to "Create".
# MAGIC * These "Shallow Cloned Tables" are "Not Self-Contained" and "Depend" on the "Source Tables" from which the "Shallow Cloned Tables" were "Cloned" as the "Source" of the "Data".
# MAGIC * If the "Data Files" in the "Source Table" that the "Shallow Cloned Table" depends on are "Removed", for example with the "VACUUM" Command, a "Shallow Cloned Table" will "No Longer" be able to "Read" the "Referenced Data Files" and a "FileNotFoundException" will be Thrown.
# MAGIC * The "Shallow Cloned Tables" are typically used for "Short-Lived Use Cases" such as - "Testing" and "Experimentation".

# COMMAND ----------

# DBTITLE 1,Create a Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS cloned_retailer_db

# COMMAND ----------

# DBTITLE 1,Create a "Shallow Cloned Table", Defined in the "Metastore", Using Spark SQL from a "Managed Delta Table"
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cloned_retailer_db.tbl_Cloned_DeltaCustomerFromDataFrame SHALLOW CLONE retailer_db.tbl_DeltaCustomerFromDataFrame

# COMMAND ----------

# DBTITLE 1,Verify the "Details" of the "Shallow Cloned Table"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED cloned_retailer_db.tbl_Cloned_DeltaCustomerFromDataFrame

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM cloned_retailer_db.tbl_Cloned_DeltaCustomerFromDataFrame WHERE TRIM(c_first_name) = 'Javier'

# COMMAND ----------

# DBTITLE 1,Update Some Values in the "Shallow Cloned Table"
# MAGIC %sql
# MAGIC UPDATE cloned_retailer_db.tbl_Cloned_DeltaCustomerFromDataFrame
# MAGIC SET c_salutation = 'Mr.'
# MAGIC WHERE TRIM(c_first_name) = 'Javier'

# COMMAND ----------

# DBTITLE 1,Verify If the "Changes" in the "Shallow Cloned Table" Has Any Effect in the "Source Table"
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_DeltaCustomerFromDataFrame WHERE TRIM(c_first_name) = 'Javier'

# COMMAND ----------

# DBTITLE 1,"Delete" the "Data Files" of the "Source Table"
dbutils.fs.rm("/user/hive/warehouse/retailer_db.db/tbl_deltacustomerfromdataframe/part-00000-2d8faacd-4fc2-40a7-92c8-52820167b55d-c000.snappy.parquet", True)
dbutils.fs.rm("/user/hive/warehouse/retailer_db.db/tbl_deltacustomerfromdataframe/part-00001-45bab1e6-8129-424c-aec5-f1fc9d20f9bb-c000.snappy.parquet", True)
dbutils.fs.rm("/user/hive/warehouse/retailer_db.db/tbl_deltacustomerfromdataframe/part-00002-9888dcd3-9749-4e2a-8f00-29f2b875cbd6-c000.snappy.parquet", True)
dbutils.fs.rm("/user/hive/warehouse/retailer_db.db/tbl_deltacustomerfromdataframe/part-00003-0653e9b5-f402-4f22-b7d5-63664a40158a-c000.snappy.parquet", True)

# COMMAND ----------

# DBTITLE 1,After "Deleting" the "Data Files" of the "Source Table", Try to Perform "SELECT" Operation on the "Shallow Cloned Table - Not Allowed"
# MAGIC %sql
# MAGIC SELECT * FROM cloned_retailer_db.tbl_Cloned_DeltaCustomerFromDataFrame

# COMMAND ----------

# DBTITLE 1,Create a "Shallow Cloned Table", Defined BY "Path", Using Spark SQL from a "Managed Delta Table"
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta. `dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableUsingSparkSql` SHALLOW CLONE retailer_db.tbl_DeltaCustomerFromDataFrame

# COMMAND ----------

# DBTITLE 1,Verify the "Details" of the "Shallow Cloned Delta Table", Created in "Path", "dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableUsingSparkSql" 
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableUsingSparkSql`
# MAGIC -- This "Shallow Cloned Delta Table" is "Not Stored" in the "Hive Metastore".

# COMMAND ----------

# DBTITLE 1,Create a "Shallow Cloned Table", Defined in the "Metastore", Using Spark SQL from a "Delta Table", Defined by "Path"
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cloned_retailer_db.tbl_Cloned_DeltaCustomerFromDataFrameWithPath SHALLOW CLONE delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`

# COMMAND ----------

# DBTITLE 1,Verify the "Details" of the "Shallow Cloned Delta Table", Defined in the "Metastore", Which is Created From a "Delta Table", Defined in "Path"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED cloned_retailer_db.tbl_Cloned_DeltaCustomerFromDataFrameWithPath
# MAGIC -- This "Shallow Cloned Delta Table" is "Stored" in the "Hive Metastore", Even Though Being "Created" From a "Delta Table" that is "Not Stored" in the "Hive Metastore".

# COMMAND ----------

# DBTITLE 1,Create a "Shallow Cloned Table", Defined by "Path", Using Spark SQL from a "Delta Table", Defined by "Path"
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta. `dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableWithPathUsingSparkSql` SHALLOW CLONE delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`

# COMMAND ----------

# DBTITLE 1,Verify the "Details" of the "Shallow Cloned Delta Table", Defined by "Path", Which is Created From a "Delta Table", Defined in "Path"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta. `dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableWithPathUsingSparkSql`
# MAGIC -- This "Shallow Cloned Delta Table" is "Not Stored" in the "Hive Metastore", and, is also "Created" From a "Delta Table" that is "Not Stored" in the "Hive Metastore".

# COMMAND ----------

# DBTITLE 1,Create a "Shallow Cloned Delta Table", Defined by "Path", Which is Created From a "Delta Table", Defined in "Path" Using Python
from delta import *

# "Create" a "Shallow Clone" of the Delta Table "retailer_db.tbl_DeltaCustomerFromDataFrame"
DeltaTable.forName(spark, "retailer_db.tbl_DeltaCustomerFromDataFrame").clone("dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableUsingPython", isShallow = True)

# Read the "Shallow Cloned Table"
display(spark.read.format("delta").load("dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableUsingPython"))

# COMMAND ----------

# DBTITLE 1,Verify the "Details" of the "Shallow Cloned Delta Table", Defined by "Path", Which is Created From a "Delta Table", Defined in "Path"
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED delta. `dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableUsingPython`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`

# COMMAND ----------

# DBTITLE 1,Create a "Shallow Cloned Table", Defined by "Path", Using Spark SQL from a "Delta Table", Defined by "Path" Using "AS OF TIMESTAMP" Option
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta. `dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableWithPathUsingSparkSqlAsOfTimestamp` SHALLOW CLONE delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath` TIMESTAMP AS OF '2022-07-17T17:07:05.000'

# COMMAND ----------

# DBTITLE 1,Create a "Shallow Cloned Table", Defined by "Path", Using Python from a "Delta Table", Defined by "Path" Using "AS OF TIMESTAMP" Option
from delta import *

timestamp_string = '2022-07-17T17:07:05.000Z'

# "Load" the Delta Table Present in the Path "/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath"
deltaTable = DeltaTable.forPath(spark, "/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath")

# "Create" a "Shallow Clone" of the Delta Table at a "Specific Timestamp"
deltaTable.cloneAtTimestamp(timestamp_string, "dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableAtTimestampUsingPython", isShallow = True)

# Read the "Shallow Cloned Table"
display(spark.read.format("delta").load("dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableAtTimestampUsingPython"))

# COMMAND ----------

# DBTITLE 1,Create a "Shallow Cloned Table", Defined by "Path", Using Spark SQL from a "Delta Table", Defined by "Path" Using "AS OF VERSION" Option
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta. `dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableWithPathUsingSparkSqlAsOfVersion` SHALLOW CLONE delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath` VERSION AS OF 0

# COMMAND ----------

# DBTITLE 1,Create a "Shallow Cloned Table", Defined by "Path", Using Python from a "Delta Table", Defined by "Path" Using "AS OF VERSION" Option
from delta import *

version = 0

# "Load" the Delta Table Present in the Path "/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath"
deltaTable = DeltaTable.forPath(spark, "/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath")

# "Create" a "Shallow Clone" of the Delta Table at a "Specific Version"
deltaTable.cloneAtVersion(version, "dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableAtVersionUsingPython", isShallow = True)

# Read the "Shallow Cloned Table"
display(spark.read.format("delta").load("dbfs:/tmp/tables/clone-examples/tbl_ShallowCloneDeltaTableAtVersionUsingPython"))

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Deep Cloned Tables"?
# MAGIC * A "Deep Cloned Table" makes a "Full Copy" of the "Metadata" and "Data Files" of the "Source Table" being "Cloned".
# MAGIC * It is similar in functionality to "Copying" with a "CTAS Command" (CREATE TABLE.. AS… SELECT…). But it is simpler to specify since it makes a "Faithful Copy" of the "Source Table" at the "Specified Version" and there is no need to "Re-specify" the "Partitioning", "Constraints" and other information as needed to do with CTAS.
# MAGIC * In addition, "Creation" of a "Deep Cloned Table" is much "Faster", "Robust", and can work in an "Incremental" Manner against "Failures".
# MAGIC * With "Deep Cloned Tables", the "Additional Metadata" are "Copied", such as the "Streaming Application Transactions" and "COPY INTO Transactions", so that the "ETL Applications" can be "Continued" exactly where it "Left Off" on a "Deep Cloned Table".

# COMMAND ----------

# DBTITLE 1,Create a "Deep Cloned Table", Defined in the "Metastore", Using Spark SQL from a "Managed Delta Table"
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cloned_retailer_db.tbl_Deep_Cloned_DeltaCustomerFromDataFrame CLONE retailer_db.tbl_DeltaCustomerFromDataFrame

# COMMAND ----------

# DBTITLE 1,Verify the "Details" of the "Deep Cloned Table"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED cloned_retailer_db.tbl_Deep_Cloned_DeltaCustomerFromDataFrame

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM cloned_retailer_db.tbl_Deep_Cloned_DeltaCustomerFromDataFrame WHERE TRIM(c_first_name) = 'Javier'

# COMMAND ----------

# DBTITLE 1,Update Some Values in the "Deep Cloned Table"
# MAGIC %sql
# MAGIC UPDATE cloned_retailer_db.tbl_Deep_Cloned_DeltaCustomerFromDataFrame
# MAGIC SET c_salutation = 'Mr.'
# MAGIC WHERE TRIM(c_first_name) = 'Javier'

# COMMAND ----------

# DBTITLE 1,Verify If the "Changes" in the "Deep Cloned Table" Has Any Effect in the "Source Table"
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_DeltaCustomerFromDataFrame WHERE TRIM(c_first_name) = 'Javier'

# COMMAND ----------

# DBTITLE 1,"Delete" the "Data Files" of the "Source Table"
dbutils.fs.rm("/user/hive/warehouse/retailer_db.db/tbl_deltacustomerfromdataframe/part-00000-d0891031-f05d-40c1-a3a9-c185b7b4ccbc-c000.snappy.parquet", True)
dbutils.fs.rm("/user/hive/warehouse/retailer_db.db/tbl_deltacustomerfromdataframe/part-00001-6a495713-4046-4999-80b5-5fa9949edf80-c000.snappy.parquet", True)
dbutils.fs.rm("/user/hive/warehouse/retailer_db.db/tbl_deltacustomerfromdataframe/part-00002-78e71638-ce19-401e-8279-ec9feb3067f2-c000.snappy.parquet", True)
dbutils.fs.rm("/user/hive/warehouse/retailer_db.db/tbl_deltacustomerfromdataframe/part-00003-40927234-1b8c-4fce-a61e-9d2e1907cd35-c000.snappy.parquet", True)

# COMMAND ----------

# DBTITLE 1,After "Deleting" the "Data Files" of the "Source Table", Try to Perform "SELECT" Operation on the "Deep Cloned Table - Not Allowed"
# MAGIC %sql
# MAGIC SELECT * FROM cloned_retailer_db.tbl_Deep_Cloned_DeltaCustomerFromDataFrame

# COMMAND ----------

# DBTITLE 1,Create a "Deep Cloned Table", Defined BY "Path", Using Spark SQL from a "Managed Delta Table"
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta. `dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableUsingSparkSql` CLONE retailer_db.tbl_DeltaCustomerFromDataFrame

# COMMAND ----------

# DBTITLE 1,Verify the "Details" of the "Deep Cloned Delta Table", Created in "Path", "dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableUsingSparkSql" 
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableUsingSparkSql`
# MAGIC -- This "Deep Cloned Delta Table" is "Not Stored" in the "Hive Metastore".

# COMMAND ----------

# DBTITLE 1,Create a "Deep Cloned Table", Defined in the "Metastore", Using Spark SQL from a "Delta Table", Defined by "Path"
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cloned_retailer_db.tbl_Deep_Cloned_DeltaCustomerFromDataFrameWithPath CLONE delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`

# COMMAND ----------

# DBTITLE 1,Verify the "Details" of the "Deep Cloned Delta Table", Defined in the "Metastore", Which is Created From a "Delta Table", Defined in "Path"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED cloned_retailer_db.tbl_Deep_Cloned_DeltaCustomerFromDataFrameWithPath
# MAGIC -- This "Deep Cloned Delta Table" is "Stored" in the "Hive Metastore", Even Though Being "Created" From an "Unmanaged Delta Table".

# COMMAND ----------

# DBTITLE 1,Create a "Deep Cloned Table", Defined by "Path", Using Spark SQL from a "Delta Table", Defined by "Path"
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta. `dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableWithPathUsingSparkSql` CLONE delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`

# COMMAND ----------

# DBTITLE 1,Verify the "Details" of the "Deep Cloned Delta Table", Defined by "Path", Which is Created From a "Delta Table", Defined in "Path"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta. `dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableWithPathUsingSparkSql`
# MAGIC -- This "Deep Cloned Delta Table" is "Not Stored" in the "Hive Metastore", and, is also "Created" From an "Unmanaged Delta Table".

# COMMAND ----------

# DBTITLE 1,Create a "Deep Cloned Delta Table", Defined by "Path", Which is Created From a "Delta Table", Defined in "Path" Using Python
from delta import *

# "Create" a "Deep Clone" of the Delta Table "retailer_db.DeltaCustomerFromDataFrame"
DeltaTable.forName(spark, "retailer_db.tbl_DeltaCustomerFromDataFrame").clone("dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableUsingPython", isShallow = False)

# Read the "Deep Cloned Table"
display(spark.read.format("delta").load("dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableUsingPython"))

# COMMAND ----------

# DBTITLE 1,Verify the "Details" of the "Deep Cloned Delta Table", Defined by "Path", Which is Created From a "Delta Table", Defined in "Path"
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED delta. `dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableUsingPython`;

# COMMAND ----------

# DBTITLE 1,Create a "Deep Cloned Table", Defined by "Path", Using Spark SQL from a "Delta Table", Defined by "Path" Using "AS OF TIMESTAMP" Option
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta. `dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableWithPathUsingSparkSqlAsOfTimestamp` CLONE delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath` TIMESTAMP AS OF '2022-07-17T17:07:05.000'

# COMMAND ----------

# DBTITLE 1,Create a "Deep Cloned Table", Defined by "Path", Using Python from a "Delta Table", Defined by "Path" Using "AS OF TIMESTAMP" Option
from delta import *

timestamp_string = '2022-07-17T17:07:05.000Z'

# "Load" the Delta Table Present in the Path "/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath"
deltaTable = DeltaTable.forPath(spark, "/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath")

# "Create" a "Shallow Clone" of the Delta Table at a "Specific Timestamp"
deltaTable.cloneAtTimestamp(timestamp_string, "dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableAtTimestampUsingPython", isShallow = False)

# Read the "Shallow Cloned Table"
display(spark.read.format("delta").load("dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableAtTimestampUsingPython"))

# COMMAND ----------

# DBTITLE 1,Create a "Deep Cloned Table", Defined by "Path", Using Spark SQL from a "Delta Table", Defined by "Path" Using "AS OF VERSION" Option
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta. `dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableWithPathUsingSparkSqlAsOfVersion` CLONE delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath` VERSION AS OF 0

# COMMAND ----------

# DBTITLE 1,Create a "Deep Cloned Table", Defined by "Path", Using Python from a "Delta Table", Defined by "Path" Using "AS OF VERSION" Option
from delta import *

version = 0

# "Load" the Delta Table Present in the Path "/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath"
deltaTable = DeltaTable.forPath(spark, "/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath")

# "Create" a "Shallow Clone" of the Delta Table at a "Specific Version"
deltaTable.cloneAtVersion(version, "dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableAtVersionUsingPython", isShallow = False)

# Read the "Shallow Cloned Table"
display(spark.read.format("delta").load("dbfs:/tmp/tables/clone-examples/tbl_DeepCloneDeltaTableAtVersionUsingPython"))

# COMMAND ----------

# MAGIC %md
# MAGIC # "Data Archiving" in "Delta Table"
# MAGIC * "Data" may need to be Kept for Longer than is feasible with "Time Travel" or for "Disaster Recovery".
# MAGIC * In these cases, it is possible to "Create" a "Deep Clone" to Preserve the "State" of a "Delta Table" at a Certain Point in "Time" for "Archival".
# MAGIC * "Incremental Archiving" is also possible to Keep a "Continually Updating State" of a "Source Table" for "Disaster Recovery".

# COMMAND ----------

# DBTITLE 1,Create a Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS archive_retailer_db

# COMMAND ----------

# DBTITLE 1,Create "Archive Table" by Assigning Values to the "logRetentionDuration" and "deletedFileRetentionDuration" Properties Using Spark SQL
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS archive_retailer_db.tbl_ArchiveCustomerWithDeltaFile
# MAGIC CLONE retailer_db.tbl_DeltaCustomerFromDataFrame
# MAGIC TBLPROPERTIES (
# MAGIC   delta.logRetentionDuration = '200 days',
# MAGIC   delta.deletedFileRetentionDuration = '200 days'
# MAGIC )
# MAGIC LOCATION 'dbfs:/tmp/tables/archive/archive_retailer_db.db/tbl_ArchiveCustomerWithDeltaFile'

# COMMAND ----------

# DBTITLE 1,Verify the "Details" of the "Deep Cloned Archive Delta Table", Defined by "Path"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta. `dbfs:/tmp/tables/archive/archive_retailer_db.db/tbl_ArchiveCustomerWithDeltaFile`

# COMMAND ----------

# DBTITLE 1,Create "Archive Table" by Assigning Values to the "logRetentionDuration" and "deletedFileRetentionDuration" Properties Using Python
from delta import *

tblProperties = {
    "delta.logRetentionDuration" : "200 days",
    "delta.deletedFileRetentionDuration" : "200 days"
}

DeltaTable.forName(spark, "retailer_db.tbl_DeltaCustomerFromDataFrame").clone("dbfs:/tmp/tables/archive/archive_retailer_db.db/tbl_ArchiveCustomerWithDeltaFileUsingPython", isShallow = False, properties = tblProperties)

# COMMAND ----------

# DBTITLE 1,Verify the "Details" of the "Deep Cloned Archive Delta Table", Defined by "Path"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta. `dbfs:/tmp/tables/archive/archive_retailer_db.db/tbl_ArchiveCustomerWithDeltaFileUsingPython`
