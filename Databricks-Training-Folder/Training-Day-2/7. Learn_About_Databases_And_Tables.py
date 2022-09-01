# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 2
# MAGIC * Topic: How to "Create" and "Display" the "Contents" of "Databases" and "Tables" Using Spark SQL in Databricks.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,Try to Display the "Records" that a "TEMPORARY VIEW" Shows Which is Created in Another Notebook - Not Allowed
display(spark.sql("SELECT * FROM v_temp_AK_Addresses"))

# COMMAND ----------

# DBTITLE 1,Try to Display the "Records" that a "GLOBAL TEMPORARY VIEW" Shows Which is Created in Another Notebook
display(spark.sql("SELECT * FROM global_temp.gv_temp_customerAddress"))

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Database"?
# MAGIC * A "Database" is a "Collection" of "Data Objects", such as "Tables" or "Views" (also called "Relations"), and "Functions".
# MAGIC * In Databricks, the terms "Schema" and "Database" are used interchangeably.
# MAGIC * In DBFS, the Created "Database" is Displayed as a "Directory" in the Path - "/user/hive/warehouse/'database-name.db'", if No "Cloud Object Storage" is "Provided".

# COMMAND ----------

# DBTITLE 1,Create a Database
spark.sql("CREATE DATABASE IF NOT EXISTS retailer_db")

# COMMAND ----------

# DBTITLE 1,Display All the Available Databases in the Databricks Environment
display(spark.sql("SHOW DATABASES"))

# COMMAND ----------

# DBTITLE 1,Know Which Database is In Use Currently
display(spark.sql("SELECT CURRENT_DATABASE()"))

# COMMAND ----------

# DBTITLE 1,Start Using Another Database Than the "default" Database in the Databricks Environment
spark.sql("USE retailer_db")

# COMMAND ----------

# DBTITLE 1,Delete a Database
spark.sql("DROP DATABASE retailer_db")

# COMMAND ----------

# MAGIC %md
# MAGIC # "Database" in "Cloud Object Storage" -
# MAGIC * "Databases" will always be associated with a "Location" on "Cloud Object Storage". It is possible to optionally specify a LOCATION when "Registering" a "Database", keeping in mind that:
# MAGIC * A. The LOCATION associated with a "Database" is always considered a "Managed Location".
# MAGIC * B. Creating a "Database" does not create any files in the "Target Location".
# MAGIC * C. The LOCATION of a "Database" will determine the "Default Location" for "Data" of "All Tables" registered to that "Database".
# MAGIC * Successfully "Dropping" a "Database" will "Recursively Drop" "All Data" and "Files" stored in a "Managed Location".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Table"?
# MAGIC * A Databricks "Table" is a "Collection" of "Structured Data".
# MAGIC * In DBFS, the "Data" of the created "Managed Table" is Stored in the Path - "/user/hive/warehouse/'database-name.db'/'table-name'/'part-files'", if LOCATION is Not specified.
# MAGIC * There are two kinds of "Tables" in "Databricks" -
# MAGIC * A. Managed Tables
# MAGIC * B. Unmanaged or External Tables
# MAGIC * All "Tables" created in "Databricks" are "Delta Tables", by default.

# COMMAND ----------

# DBTITLE 1,Create a Table from a Global Temporary View
spark.sql("CREATE TABLE IF NOT EXISTS tbl_demo AS SELECT * FROM global_temp.gv_temp_customerAddress")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL retailer_db.tbl_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED retailer_db.tbl_demo

# COMMAND ----------

# DBTITLE 1,Display All Available Tables in the Current Database in Use in the Databricks Environment
display(spark.sql("SHOW TABLES"))

# COMMAND ----------

# DBTITLE 1,Display All Available Tables in a Particular Database in a Databricks Environment
display(spark.sql("SHOW TABLES IN retailer_db"))

# COMMAND ----------

# MAGIC %md
# MAGIC # What is an "Unmanaged Table" or "External Table"?
# MAGIC * Databricks only "Manages" the "Metadata" for the "Unmanaged" or "External" Tables.
# MAGIC * When an "Unmanaged" or "External" Table is "Dropped", the "Underlying Data" is "Not Affected".
# MAGIC * "Unmanaged" or "External" Tables will always specify a LOCATION during the "Table Creation".
# MAGIC * Either an "Existing Directory of Data Files" can be "Registered"  as an "Unmanaged" or "External" Table or a "Path" can be provide  when an "Unmanaged" or "External" Table is first defined.
# MAGIC * Because "Data" and "Metadata" are "Managed Independently", it is possible to "Rename" an "Unmanaged" or "External" Table or "Register" an "Unmanaged" or "External" Table to a "New Database" without needing to move any "Data".
# MAGIC * "Data Engineers" often prefer "Unmanaged" or "External" Tables and the flexibility these provide for "Production Data".

# COMMAND ----------

# DBTITLE 1,Create External Table from CSV Using Spark SQL
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retailer_db.tbl_CustomerDatFile
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
# MAGIC USING csv
# MAGIC OPTIONS
# MAGIC (
# MAGIC     path 'dbfs:/FileStore/tables/retailer/data/customer.dat',
# MAGIC     sep '|',
# MAGIC     header true
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Verify if "retailer_db.tbl_CustomerDatFile" is "Unmanaged", i.e., "External"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED retailer_db.tbl_CustomerDatFile

# COMMAND ----------

# DBTITLE 1,Create External Table from CSV Using Spark SQL by Optionally Specifying "LOCATION" - Not Allowed
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retailer_db.tbl_CustomerDatFileWithLocation
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
# MAGIC USING csv
# MAGIC LOCATION 'dbfs:/customTablePath'
# MAGIC OPTIONS
# MAGIC (
# MAGIC     path 'dbfs:/FileStore/tables/retailer/data/customer.dat',
# MAGIC     sep '|',
# MAGIC     header true
# MAGIC )
# MAGIC 
# MAGIC /* Duplicated table paths found: 'dbfs:/customTablePath' and 'dbfs:/FileStore/tables/retailer/data/customer.dat'. LOCATION and the case insensitive key 'path' in OPTIONS are all used to indicate the custom table path, you can only specify one of them. - This "Exception" will be thrown.

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerFileUsingCsv = spark.read\
                                    .option("header", "true")\
                                    .option("inferSchema", "true")\
                                    .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")
display(df_ReadCustomerFileUsingCsv)

# COMMAND ----------

# DBTITLE 1,Create External Table from a DataFrame Using "saveAsTable" Method in Python
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

df_ReadCustomerFileUsingCsv.write\
                           .options(
                                      path = "/tmp/tables/retailer_db.db/tbl_FromCustomerDataFrame",\
                                      schema = ddl\
                                   )\
                           .saveAsTable("retailer_db.tbl_FromCustomerDataFrame")

# COMMAND ----------

# DBTITLE 1,Verify if "retailer_db.tbl_FromCustomerDataFrame" is "Unmanaged", i.e., "External" Using "Spark SQL"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED retailer_db.tbl_FromCustomerDataFrame

# COMMAND ----------

# DBTITLE 1,Rename "Unmanaged Tables" Using Spark SQL
# MAGIC %sql
# MAGIC ALTER TABLE retailer_db.tbl_FromCustomerDataFrame RENAME TO retailer_db.tbl_RenamedFromCustomerDataFrame;

# COMMAND ----------

# DBTITLE 1,Verify if the Path of the "Unmanaged" Table is Changed After Renaming
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED retailer_db.tbl_RenamedFromCustomerDataFrame

# COMMAND ----------

# DBTITLE 1,Create a New Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS new_retailer_db

# COMMAND ----------

# DBTITLE 1,Drop an "Unmanaged" or "External Table"
# MAGIC %sql
# MAGIC DROP TABLE retailer_db.tbl_RenamedFromCustomerDataFrame

# COMMAND ----------

# DBTITLE 1,Create Another "Unmanaged Table" Using the "Delta Files" of a Deleted "Unmanaged Table"
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retailer_db.tbl_CustomerWithDeltaFile
# MAGIC (
# MAGIC     c_customer_sk int,
# MAGIC     c_customer_id string,
# MAGIC     c_current_cdemo_sk int,
# MAGIC     c_current_hdemo_sk int,
# MAGIC     c_current_addr_sk int,
# MAGIC     c_first_shipto_date_sk int,
# MAGIC     c_first_sales_date_sk int,
# MAGIC     c_salutation string,
# MAGIC     c_first_name string,
# MAGIC     c_last_name string,
# MAGIC     c_preferred_cust_flag string,
# MAGIC     c_birth_day int,
# MAGIC     c_birth_month int,
# MAGIC     c_birth_year int,
# MAGIC     c_birth_country string,
# MAGIC     c_login string,
# MAGIC     c_email_address string,
# MAGIC     c_last_review_date double
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'dbfs:/tmp/tables/retailer_db.db/tbl_FromCustomerDataFrame'

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Managed Table"?
# MAGIC * Databricks "Manages" both the "Metadata" and the "Data" for a "Managed Table".
# MAGIC * When a "Managed Table" is "Dropped", the "Underlying Data" is also "Deleted". "Data Analysts" and other "Users" that mostly work in SQL may prefer this behavior.
# MAGIC * "Managed Tables" are the "Default" when creating a "Table".
# MAGIC * The "Data" for a "Managed Table" resides in the LOCATION of the "Database" it is "Registered To".
# MAGIC * This "Managed Relationship" between the "Data Location" and the "Database" means that in order to "Move" a "Managed Table" to a "New Database", "All Data" must be "Rewritten" to the "New Location".

# COMMAND ----------

# DBTITLE 1,Create a "Managed Table" Using "saveAsTable ()" Method of the "DataFrameWriter"
ddl = """ca_address_sk long comment \"This is the Primary Key\",
         ca_address_id string,
         ca_street_number long,
         ca_street_name string,
         ca_street_type string,
		 ca_suite_number string,
		 ca_city string,
		 ca_county string,
		 ca_state string,
		 ca_zip long,
		 ca_country string,
		 ca_gmt_offset int,
		 ca_location_type string"""

df_ReadCustomerAddress = spark.read.options(\
                                                header = "true",\
                                                sep = "|",\
                                                schema = ddl\
                                           )\
                                   .csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")

df_ReadCustomerAddress.write.saveAsTable("retailer_db.tbl_ManagedCustomerAddress")
display(spark.sql("select * from retailer_db.tbl_ManagedCustomerAddress"))

# COMMAND ----------

# DBTITLE 1,Verify if "retailer_db.tbl_ManagedCustomerAddress" is "Managed"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED retailer_db.tbl_ManagedCustomerAddress

# COMMAND ----------

# DBTITLE 1,Create a "Managed Table" By Creating an "Empty Table" First and Then Loading Data Into It
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retailer_db.tbl_ManagedCustomerWithAddress
# MAGIC (
# MAGIC     ca_address_sk STRING,
# MAGIC     ca_address_id STRING,
# MAGIC     ca_street_number STRING,
# MAGIC     ca_street_name STRING,
# MAGIC     ca_street_type STRING,
# MAGIC     ca_suite_number STRING,
# MAGIC     ca_city STRING,
# MAGIC     ca_county STRING,
# MAGIC     ca_state STRING,
# MAGIC     ca_zip STRING,
# MAGIC     ca_country STRING,
# MAGIC     ca_gmt_offset STRING,
# MAGIC     ca_location_type STRING
# MAGIC );
# MAGIC 
# MAGIC SELECT * FROM retailer_db.tbl_ManagedCustomerWithAddress

# COMMAND ----------

# DBTITLE 1,Load Data Into the Created "Empty Managed Table"
# MAGIC %sql
# MAGIC TRUNCATE TABLE retailer_db.tbl_ManagedCustomerWithAddress;
# MAGIC 
# MAGIC INSERT INTO retailer_db.tbl_ManagedCustomerWithAddress
# MAGIC SELECT      ca_address_sk,
# MAGIC             ca_address_id,
# MAGIC             ca_street_number,
# MAGIC             ca_street_name,
# MAGIC             ca_street_type,
# MAGIC             ca_suite_number,
# MAGIC             ca_city,
# MAGIC             ca_county,
# MAGIC             ca_state,
# MAGIC             ca_zip,
# MAGIC             ca_country,
# MAGIC             ca_gmt_offset,
# MAGIC             ca_location_type
# MAGIC 
# MAGIC FROM        retailer_db.tbl_ManagedCustomerAddress MCA
# MAGIC INNER JOIN  retailer_db.tbl_CustomerDatFile CDF
# MAGIC ON          MCA.ca_address_sk = CDF.c_current_addr_sk;
# MAGIC 
# MAGIC SELECT * FROM retailer_db.tbl_ManagedCustomerWithAddress

# COMMAND ----------

# DBTITLE 1,Verify if "retailer_db.tbl_ManagedCustomerWithAddress" is "Managed"
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED retailer_db.tbl_ManagedCustomerWithAddress

# COMMAND ----------

# DBTITLE 1,Display Metadata About All the Columns of a Table
display(spark.sql("DESCRIBE retailer_db.tbl_CustomerDatFile"))

# COMMAND ----------

# DBTITLE 1,Display Metadata About the Table
display(spark.sql("DESCRIBE FORMATTED retailer_db.tbl_CustomerDatFile"))
