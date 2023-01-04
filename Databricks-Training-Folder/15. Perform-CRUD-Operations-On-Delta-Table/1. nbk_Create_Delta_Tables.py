# Databricks notebook source
# MAGIC %md
# MAGIC # Create New Delta Tables in Databricks
# MAGIC * Topic: How to Create "Delta Tables"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # How to Create "Delta Tables"?
# MAGIC * "Delta Lake" supports creating "Two Types" of "Tables" -
# MAGIC * A. "Tables" defined in the "Metastore".
# MAGIC * B. "Tables" defined by "Path" ("Delta Table" is "Created" at a "Path" without "Creating" an "Entry" in the "Hive Metastore").

# COMMAND ----------

# MAGIC %md
# MAGIC # Using "SQL DDL Commands"
# MAGIC * It is possible to use "Standard SQL DDL Commands" supported in "Apache Spark" (for example, CREATE TABLE and REPLACE TABLE) to Create "Delta Tables".
# MAGIC * In Databricks Runtime 8.0 and above, "Delta Lake" is the "Default Format" and "USING DELTA" is not needed.

# COMMAND ----------

# DBTITLE 1,Create an Empty Delta Table, Defined in the "Metastore", Using "CREATE TABLE" SQL Command
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retailer_db.tbl_DeltaCustomer
# MAGIC (
# MAGIC     c_customer_sk LONG,
# MAGIC     c_customer_id STRING,
# MAGIC     c_current_cdemo_sk LONG,
# MAGIC     c_current_hdemo_sk LONG,
# MAGIC     c_current_addr_sk LONG,
# MAGIC     c_first_shipto_date_sk LONG,
# MAGIC     c_first_sales_date_sk LONG,
# MAGIC     c_salutation STRING,
# MAGIC     c_first_name STRING,
# MAGIC     c_last_name STRING,
# MAGIC     c_preferred_cust_flag STRING,
# MAGIC     c_birth_day INT,
# MAGIC     c_birth_month INT,
# MAGIC     c_birth_year INT,
# MAGIC     c_birth_country STRING,
# MAGIC     c_login STRING,
# MAGIC     c_email_address STRING,
# MAGIC     c_last_review_date LONG
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Verify if "retailer_db.tbl_DeltaCustomer" is "Managed"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED retailer_db.tbl_DeltaCustomer

# COMMAND ----------

# DBTITLE 1,Create an Empty Delta Table, Defined in the "Metastore", Using "CREATE OR REPLACE TABLE" SQL Command
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retailer_db.tbl_DeltaCustomer
# MAGIC (
# MAGIC     c_customer_sk LONG,
# MAGIC     c_customer_id STRING,
# MAGIC     c_current_cdemo_sk LONG,
# MAGIC     c_current_hdemo_sk LONG,
# MAGIC     c_current_addr_sk LONG,
# MAGIC     c_first_shipto_date_sk LONG,
# MAGIC     c_first_sales_date_sk LONG,
# MAGIC     c_salutation STRING,
# MAGIC     c_first_name STRING,
# MAGIC     c_last_name STRING,
# MAGIC     c_preferred_cust_flag STRING,
# MAGIC     c_birth_day INT,
# MAGIC     c_birth_month INT,
# MAGIC     c_birth_year INT,
# MAGIC     c_birth_country STRING,
# MAGIC     c_login STRING,
# MAGIC     c_email_address STRING,
# MAGIC     c_last_review_date LONG
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Verify if "retailer_db.tbl_DeltaCustomer" is "Managed"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED retailer_db.tbl_DeltaCustomer

# COMMAND ----------

# DBTITLE 1,Create an Empty Delta Table, Defined by "Path", Without Storing in the "Hive Metastore"
# MAGIC %sql
# MAGIC /* In Databricks Runtime 7.0 and above, Spark SQL also supports a creating "Delta Table" at a "Path" without creating an "Entry" in the "Hive Metastore". */
# MAGIC CREATE OR REPLACE TABLE delta. `/tmp/tables/delta/tbl_DeltaCustomerWithOnlyPath`
# MAGIC (
# MAGIC     c_customer_sk LONG,
# MAGIC     c_customer_id STRING,
# MAGIC     c_current_cdemo_sk LONG,
# MAGIC     c_current_hdemo_sk LONG,
# MAGIC     c_current_addr_sk LONG,
# MAGIC     c_first_shipto_date_sk LONG,
# MAGIC     c_first_sales_date_sk LONG,
# MAGIC     c_salutation STRING,
# MAGIC     c_first_name STRING,
# MAGIC     c_last_name STRING,
# MAGIC     c_preferred_cust_flag STRING,
# MAGIC     c_birth_day INT,
# MAGIC     c_birth_month INT,
# MAGIC     c_birth_year INT,
# MAGIC     c_birth_country STRING,
# MAGIC     c_login STRING,
# MAGIC     c_email_address STRING,
# MAGIC     c_last_review_date LONG
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

dbutils.fs.rm("/user/hive/warehouse/retailer_db.db", True)

# COMMAND ----------

# DBTITLE 1,"Delta Tables", Created By "Path", is "Not Stored" in the "Hive Metastore"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta. `/tmp/tables/delta/tbl_DeltaCustomerWithOnlyPath`

# COMMAND ----------

# MAGIC %md
# MAGIC # Using "DataFrameWriter API"
# MAGIC * To simultaneously "Create" a "Delta Table" and "Insert" the "Data" into it from "Spark DataFrames", the Spark "DataFrameWriter" API is used.
# MAGIC * In Databricks Runtime 8.0 and above, "Delta Lake" is the "Default Format" and there is no need to specify "format("delta")"", or "using("delta")".

# COMMAND ----------

# DBTITLE 1,Create Delta Table, Defined in the "Metastore", Using DataFrameWriter API in the "Default" Path
# Create "Delta Table" in the "Hive Metastore" using DataFrame's "Schema" and "Write" the "Data" to it.
ddl = """c_customer_sk long comment \"This is the Primary Key\",
         c_customer_id string,
         c_current_cdemo_sk long,
         c_current_hdemo_sk long,
         c_current_addr_sk long,
         c_first_shipto_date_sk long,
         c_first_sales_date_sk long,
         c_salutation string,
         c_first_name string,
         c_last_name string,
         c_preferred_cust_flag string,
         c_birth_day int,
         c_birth_month int,
         c_birth_year int,
         c_birth_country string,
         c_login string,
         c_email_address string,
         c_last_review_date long"""

df_ReadCustomerFileUsingCsv = spark.read\
                                   .option("header", "true")\
                                   .schema(ddl)\
                                   .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")

df_ReadCustomerFileUsingCsv.printSchema()

df_ReadCustomerFileUsingCsv.write.format("delta").mode("append").saveAsTable("retailer_db.tbl_DeltaCustomerFromDataFrame")

# The "Files" for the "Delta Table" is "Created" in the Default Path "/user/hive/warehouse/retailer_db.db/tbl_deltacustomerfromdataframe".

# COMMAND ----------

# DBTITLE 1,Verify if "retailer_db.tbl_DeltaCustomerFromDataFrame" is "Managed"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED retailer_db.tbl_DeltaCustomerFromDataFrame

# COMMAND ----------

# DBTITLE 1,Create Delta Table, Defined by "Path", Using DataFrameWriter API in a "Provided" Path Without Storing in the "Hive Metastore"
# Create or Replace "Delta Table" in the "Hive Metastore" with "Path" using DataFrame's Schema and "Write"/"Overwrite" the "Data" to it.
ddl = """c_customer_sk long comment \"This is the Primary Key\",
         c_customer_id string,
         c_current_cdemo_sk long,
         c_current_hdemo_sk long,
         c_current_addr_sk long,
         c_first_shipto_date_sk long,
         c_first_sales_date_sk long,
         c_salutation string,
         c_first_name string,
         c_last_name string,
         c_preferred_cust_flag string,
         c_birth_day int,
         c_birth_month int,
         c_birth_year int,
         c_birth_country string,
         c_login string,
         c_email_address string,
         c_last_review_date long"""

df_ReadCustomerFileUsingCsv = spark.read\
                                   .option("header", "true")\
                                   .schema(ddl)\
                                   .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")

df_ReadCustomerFileUsingCsv.printSchema()

df_ReadCustomerFileUsingCsv.write.format("delta").mode("overwrite").save("/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath")

# The "Files" for the "Delta Table" is "Created" in the Default Path "/user/hive/warehouse/retailer_db.db/tbl_deltacustomerfromdataframe".

# COMMAND ----------

# DBTITLE 1,"Delta Tables", Created By "Path", is "Not Stored" in the "Hive Metastore"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`

# COMMAND ----------

# MAGIC %md
# MAGIC # Using "DeltaTableBuilder API"
# MAGIC * It is possible to use the "DeltaTableBuilder API" in the "Delta Lake" to "Create" the "Delta Tables".
# MAGIC * Compared to the "DataFrameWriter APIs", this API makes it easier to "Specify Additional Information" like "Column Comments", "Table Properties", and "Generated Columns".

# COMMAND ----------

# DBTITLE 1,Create Empty Delta Table, Defined in the "Metastore", Using DeltaTableBuilder API in the "Default" Path
from delta import *

DeltaTable.createIfNotExists(spark)\
  .tableName("retailer_db.tbl_DeltaCustomerFromDeltaTableBuilder")\
  .addColumn("c_customer_sk", "LONG", comment = "This is the Primary Key")\
  .addColumn("c_customer_id", "STRING")\
  .addColumn("c_current_cdemo_sk", "LONG")\
  .addColumn("c_current_hdemo_sk", "LONG")\
  .addColumn("c_current_addr_sk", "LONG")\
  .addColumn("c_first_shipto_date_sk", "LONG")\
  .addColumn("c_first_sales_date_sk", "LONG")\
  .addColumn("c_salutation", "STRING")\
  .addColumn("c_first_name", "STRING")\
  .addColumn("c_last_name", "STRING")\
  .addColumn("c_preferred_cust_flag", "STRING")\
  .addColumn("c_birth_day", "INT")\
  .addColumn("c_birth_month", "INT")\
  .addColumn("c_birth_year", "INT")\
  .addColumn("c_birth_country", "STRING")\
  .addColumn("c_login", "STRING")\
  .addColumn("c_email_address", "STRING")\
  .addColumn("c_last_review_date", "LONG")\
  .execute()

# COMMAND ----------

# DBTITLE 1,Verify if "retailer_db.tbl_DeltaCustomerFromDeltaTableBuilder" is "Managed"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED retailer_db.tbl_DeltaCustomerFromDeltaTableBuilder

# COMMAND ----------

# DBTITLE 1,Create Empty Delta Table, Defined by "Path", Using DeltaTableBuilder API in a "Provided" Path Without Storing in the "Hive Metastore"
from delta import *

DeltaTable.createOrReplace(spark)\
  .addColumn("c_customer_sk", "LONG", comment = "This is the Primary Key")\
  .addColumn("c_customer_id", "STRING")\
  .addColumn("c_current_cdemo_sk", "LONG")\
  .addColumn("c_current_hdemo_sk", "LONG")\
  .addColumn("c_current_addr_sk", "LONG")\
  .addColumn("c_first_shipto_date_sk", "LONG")\
  .addColumn("c_first_sales_date_sk", "LONG")\
  .addColumn("c_salutation", "STRING")\
  .addColumn("c_first_name", "STRING")\
  .addColumn("c_last_name", "STRING")\
  .addColumn("c_preferred_cust_flag", "STRING")\
  .addColumn("c_birth_day", "INT")\
  .addColumn("c_birth_month", "INT")\
  .addColumn("c_birth_year", "INT")\
  .addColumn("c_birth_country", "STRING")\
  .addColumn("c_login", "STRING")\
  .addColumn("c_email_address", "STRING")\
  .addColumn("c_last_review_date", "LONG")\
  .property("description", "Customer Delta Table Created without Storing in Hive Metastore Using DeltaTableBuilder API")\
  .location("/tmp/tables/delta/tbl_DeltaCustomerUsingDeltaTableBuilderWithPath")\
  .execute()

# COMMAND ----------

# DBTITLE 1,"Delta Tables", Created By "Path", is "Not Stored" in the "Hive Metastore"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta. `/tmp/tables/delta/tbl_DeltaCustomerUsingDeltaTableBuilderWithPath`

# COMMAND ----------

# MAGIC %md
# MAGIC # Partition Data of a Delta Table
# MAGIC * It is possible to "Partition" the "Data" to "Speed Up" the "Queries" or "DML" that have "Predicates" involving the "Partition Columns".
# MAGIC * To "Partition" the "Data", at the time of "Creating" a "Delta Table", the "partitioned by" Clause, followed by the "Columns" need to be specified.

# COMMAND ----------

# DBTITLE 1,Create an Empty Partitioned Delta Table, Defined in the "Metastore", Using "CREATE TABLE" SQL Command
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retailer_db.tbl_PartitionedDeltaCustomerUsingSparkSql
# MAGIC (
# MAGIC     c_customer_sk LONG,
# MAGIC     c_customer_id STRING,
# MAGIC     c_current_cdemo_sk LONG,
# MAGIC     c_current_hdemo_sk LONG,
# MAGIC     c_current_addr_sk LONG,
# MAGIC     c_first_shipto_date_sk LONG,
# MAGIC     c_first_sales_date_sk LONG,
# MAGIC     c_salutation STRING,
# MAGIC     c_first_name STRING,
# MAGIC     c_last_name STRING,
# MAGIC     c_preferred_cust_flag STRING,
# MAGIC     c_birth_day INT,
# MAGIC     c_birth_month INT,
# MAGIC     c_birth_year INT,
# MAGIC     c_birth_country STRING,
# MAGIC     c_login STRING,
# MAGIC     c_email_address STRING,
# MAGIC     c_last_review_date LONG
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (c_birth_year, c_birth_month, c_birth_day)

# COMMAND ----------

# DBTITLE 1,Create Partitioned Delta Table, Defined in the "Metastore", Using DataFrameWriter API in the "Default" Path
# Create "Delta Table" in the "Hive Metastore" using DataFrame's "Schema" and "Write" the "Data" to it.
ddl = """c_customer_sk long comment \"This is the Primary Key\",
         c_customer_id string,
         c_current_cdemo_sk long,
         c_current_hdemo_sk long,
         c_current_addr_sk long,
         c_first_shipto_date_sk long,
         c_first_sales_date_sk long,
         c_salutation string,
         c_first_name string,
         c_last_name string,
         c_preferred_cust_flag string,
         c_birth_day int,
         c_birth_month int,
         c_birth_year int,
         c_birth_country string,
         c_login string,
         c_email_address string,
         c_last_review_date long"""

df_ReadCustomerFileUsingCsv = spark.read\
                                   .option("header", "true")\
                                   .schema(ddl)\
                                   .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")

df_ReadCustomerFileUsingCsv.printSchema()

df_ReadCustomerFileUsingCsv.write.format("delta").partitionBy("c_birth_year", "c_birth_month", "c_birth_day").saveAsTable("retailer_db.PartitionedDeltaCustomerUsingDataFrame")

# COMMAND ----------

# DBTITLE 1,Create Empty Partitioned Delta Table, Defined in the "Metastore", Using DeltaTableBuilder API in the "Default" Path
from delta import *

DeltaTable.createIfNotExists(spark)\
  .tableName("retailer_db.tbl_PartitionedDeltaCustomerUsingTableBuilder")\
  .addColumn("c_customer_sk", "LONG", comment = "This is the Primary Key")\
  .addColumn("c_customer_id", "STRING")\
  .addColumn("c_current_cdemo_sk", "LONG")\
  .addColumn("c_current_hdemo_sk", "LONG")\
  .addColumn("c_current_addr_sk", "LONG")\
  .addColumn("c_first_shipto_date_sk", "LONG")\
  .addColumn("c_first_sales_date_sk", "LONG")\
  .addColumn("c_salutation", "STRING")\
  .addColumn("c_first_name", "STRING")\
  .addColumn("c_last_name", "STRING")\
  .addColumn("c_preferred_cust_flag", "STRING")\
  .addColumn("c_birth_day", "INT")\
  .addColumn("c_birth_month", "INT")\
  .addColumn("c_birth_year", "INT")\
  .addColumn("c_birth_country", "STRING")\
  .addColumn("c_login", "STRING")\
  .addColumn("c_email_address", "STRING")\
  .addColumn("c_last_review_date", "LONG")\
  .partitionedBy("c_birth_year", "c_birth_month", "c_birth_day")\
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # Verify If a Delta Table is Partitioned
# MAGIC * To "Determine" whether a "Delta Table" contains a "Partition", perform the following steps -
# MAGIC * A. Open the "Table" in the "DBFS" and Check in the "Details" Tab, if the "Delta Table" is "Partitioned", then the "name" of the "Columns" are Displayed in the "Partition columns:".
# MAGIC * B. Use the Command "DESCRIBE EXTENDED 'table-name'". In the section "# Partitioning", the "Partitioned Columns" are "Mentioned".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Controlling" the "Data Location" of a "Delta Table"
# MAGIC * For "Delta Tables" that are "Defined" in the "Hive Metastore", it is possible to optionally specify the LOCATION as a "Path".
# MAGIC * "Delta Tables" that are created with a specified LOCATION are considered "Unmanaged" by the "Hive Metastore". Unlike a "Managed Table", where "No Path" is specified, an "Unmanaged Table’s" Files are "Not Deleted" when the "Tables" are "Dropped".
# MAGIC * When the "CREATE TABLE" Command is "Run" with a "LOCATION" Option that already contains "Data" stored using "Delta Lake", the "Delta Lake" does the following -
# MAGIC * A. If only the "Name" and the "Location" of the "Delta Table" are specified, the "Delta Table" to be "Created" in the "Hive Metastore" automatically "Inherits" the "Schema", "Partitioning", and the "Table Properties" of the "Existing Data". This functionality can be used to "Import" the "Data" into the "Metastore".
# MAGIC * B. If any "Configuration" ("Schema", "Partitioning", or "Table Properties") is specified, then the "Delta Lake" verifies if the "Specification" exactly matches with the "Configuration" of the "Existing Data". If the specified "Configuration" does not exactly match the "Configuration" of the "Existing Data", then the "Delta Lake" throws an "Exception" that describes the "Discrepancy".

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retailer_db.tbl_CustomerWithDeltaFile
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/tmp/tables/retailer_db.db/tbl_FromCustomerDataFrame'
