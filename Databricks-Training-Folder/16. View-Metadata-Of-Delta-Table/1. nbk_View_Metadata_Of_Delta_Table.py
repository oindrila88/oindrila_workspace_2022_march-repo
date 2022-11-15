# Databricks notebook source
# MAGIC %md
# MAGIC # View Metadata of Delta Tables in Databricks
# MAGIC * Topic: How to Display the "Metadata" of the "Delta Tables"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Display "All" the "Partitions" of a "Delta Table"
# MAGIC * The "SHOW PARTITIONS" Command "Lists" the "Partitions" of a "Delta Table".

# COMMAND ----------

# DBTITLE 1,Display "All" the "Partitions" Present in the Delta Table "retailer_db.tbl_DeltaCustomer"
# MAGIC %sql
# MAGIC SHOW PARTITIONS retailer_db.PartitionedDeltaCustomerUsingDataFrame;

# COMMAND ----------

# DBTITLE 1,"Specify" a "Full Partition Specification" to "List" the "Specific Partition"
# MAGIC %sql
# MAGIC SHOW PARTITIONS retailer_db.PartitionedDeltaCustomerUsingDataFrame PARTITION (c_birth_year = 1988, c_birth_day = 2, c_birth_month = 9);

# COMMAND ----------

# DBTITLE 1,"Specify" a "Partial Partition Specification" to "List" the "Specific Partitions"
# MAGIC %sql
# MAGIC SHOW PARTITIONS retailer_db.PartitionedDeltaCustomerUsingDataFrame PARTITION (c_birth_day = 2, c_birth_month = 9);

# COMMAND ----------

# DBTITLE 1,"Specify" a "Partial Specification" to "List" the "Specific Partitions"
# MAGIC %sql
# MAGIC SHOW PARTITIONS retailer_db.PartitionedDeltaCustomerUsingDataFrame PARTITION (c_birth_day = 2);

# COMMAND ----------

# MAGIC %md
# MAGIC # Display "All" the "Columns" of a "Delta Table"
# MAGIC * The "SHOW COLUMNS" Command "Returns" the "List" of "Columns" in a "Delta Table".
# MAGIC * If the "Delta Table" "Does Not Exist", an "Exception" is "Thrown".

# COMMAND ----------

# DBTITLE 1,Display "All" the "Columns" of the Delta Table "retailer_db.tbl_DeltaCustomer"
# MAGIC %sql
# MAGIC SHOW COLUMNS IN retailer_db.tbl_DeltaCustomer;

# COMMAND ----------

# DBTITLE 1,Display "All" the "Columns" Present in the Delta Table "tbl_DeltaCustomer" of the Schema "retailer_db"
# MAGIC %sql
# MAGIC SHOW COLUMNS IN tbl_DeltaCustomer IN retailer_db;

# COMMAND ----------

# MAGIC %md
# MAGIC # Display "All" the "Metadata" of a "Delta Table"
# MAGIC * The "DESCRIBE TABLE" Command "Returns" the "Basic Metadata" Information of a "Delta Table".
# MAGIC * The "Metadata Information" includes the "Column Name", "Column Type", "Column Comment", and, the "Partition Information".

# COMMAND ----------

# DBTITLE 1,Display the "Basic Metadata" Information for the Delta Table "tbl_DeltaCustomer" of the Schema "retailer_db"
# MAGIC %sql
# MAGIC DESCRIBE TABLE retailer_db.tbl_DeltaCustomer;

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "Delta Table" of which the "Metadata" is to be Displayed, "Does Not Exist", an "Exception" is "Thrown".

# COMMAND ----------

# DBTITLE 1,"Exception" is "Thrown", If the "Delta Table" "Does Not Exist"
# MAGIC %sql
# MAGIC DESCRIBE TABLE retailer_db.tbl_New_DeltaCustomer;

# COMMAND ----------

# MAGIC %md
# MAGIC * An "Optional Parameter", i.e., "PARTITION" used with the "DESCRIBE TABLE" Command "Directs" the "Databricks Runtime" to "Return" the "Addition Metadata" for the "Named Partitions".
# MAGIC * The "Partition Specification" "Must Provide" the "Values" for "All" the "Partition Columns".

# COMMAND ----------

# DBTITLE 1,Display the "Addition Metadata" for the "Named Partition"
# MAGIC %sql
# MAGIC DESCRIBE TABLE retailer_db.PartitionedDeltaCustomerUsingDataFrame PARTITION (c_birth_year = 1988, c_birth_day = 2, c_birth_month = 9);

# COMMAND ----------

# MAGIC %md
# MAGIC * An "Optional Parameter", i.e., "COLUMN_NAME" used with the "DESCRIBE TABLE" Command "Mentions" the "Column Name" that "Needs" to be "Described".
# MAGIC * Currently "Nested Columns" are "Not Allowed" to be "Specified".

# COMMAND ----------

# DBTITLE 1,Display the "Addition Metadata" for the "Named Column"
# MAGIC %sql
# MAGIC DESCRIBE TABLE retailer_db.tbl_DeltaCustomer c_email_address;

# COMMAND ----------

# MAGIC %md
# MAGIC * Optional Parameters "PARTITION" and "COLUMN_NAME" are "Mutually Exclusive" and "Cannot" be "Specified Together" in the "DESCRIBE TABLE" Command.

# COMMAND ----------

# DBTITLE 1,Optional Parameters "PARTITION" and "COLUMN_NAME" Can't be Used Together
# MAGIC %sql
# MAGIC DESCRIBE TABLE retailer_db.PartitionedDeltaCustomerUsingDataFrame c_email_address PARTITION (c_birth_year = 1988, c_birth_day = 2, c_birth_month = 9);

# COMMAND ----------

# MAGIC %md
# MAGIC # DESCRIBE DETAIL
# MAGIC * The "DESCRIBE DETAIL" Command "Provides" the "Information" about the "Schema", "Partitioning", "Table Size", and so on.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL retailer_db.PartitionedDeltaCustomerUsingDataFrame;

# COMMAND ----------

# MAGIC %md
# MAGIC # DESCRIBE HISTORY
# MAGIC * The "DESCRIBE HISTORY" Command "Provides" the "Provenance Information", including the "Operation", "User", and so on, and "Operation Metrics" for "Each Write" to a "Delta Table".
# MAGIC * "Table History" is "Retained" for "30 Days".

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY retailer_db.tbl_managedcustomeraddress;

# COMMAND ----------

# MAGIC %md
# MAGIC # DESCRIBE EXTENDED
# MAGIC * The "DESCRIBE EXTENDED" Command Displays the "Detailed Information" about the "Delta Table", including the "Parent Database", "Table Type", "Storage Information", and "Properties".

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED retailer_db.PartitionedDeltaCustomerUsingDataFrame;

# COMMAND ----------

# MAGIC %md
# MAGIC # DESCRIBE FORMATTED
# MAGIC * The "DESCRIBE FORMATTED" Command "Returns" the "Delta Table Format".

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED retailer_db.PartitionedDeltaCustomerUsingDataFrame;
